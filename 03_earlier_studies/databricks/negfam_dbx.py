"""D-NEG arm: negative-family confirmation on real Spark (Databricks).

Runs the 4 negative-family V pipelines (PREREGISTRATION.md Amendment 4
S.A4.2) over the negfam fact table (WIDE payload rows) and compares the
synopsis-arm predictions against the executed Spark verdicts.

Pipelines and pre-registered byte thresholds (Amendment 4, locked
2026-11-06 before any negative-family run -- NOT re-fitted here):
  V1 GROUP BY bytes            max over k of SUM(LENGTH(payload))  > 4,000,000 B
  V2 fact⋈dim join bytes       max hash-partition (P=128) of
                               per-key join output bytes           > 10,000,000 B
  V3 string-partition spill    max over sprefix of SUM(LENGTH)     > 50,000,000 B
  V4 UDF cost                  max hash-partition (P=128) of
                               per-key SUM(LENGTH(payload)^2)      > 50,000,000,000

MEASUREMENT (D8 with byte aggregation): loads are BYTE loads (sum of
payload byte lengths per partition), measured via spark_partition_id().
Where the pipeline's definition is a per-key/per-sprefix max rather
than a per-partition max, BOTH quantities are executed and recorded:
  V1: verdict-bearing = executed per-key max (definition match with the
      local GT); per-partition byte sums after repartition(128, k)
      reported alongside (the D2-style realised load).
  V2: verdict-bearing = per-partition byte max of the join output
      repartitioned on k (definition match modulo the D1 hash function:
      murmur3 here vs splitmix64 locally).
  V3: verdict-bearing = executed per-sprefix max (definition match);
      per-partition byte sums after repartition(128, sprefix) reported.
  V4: verdict-bearing = per-partition max of per-key SUM(LENGTH^2)
      (definition match modulo D1).

DATA ROUTE -- UPLOAD, not regeneration (--gen-mode upload, the only
supported mode). The negfam parquet is uploaded to a Unity Catalog
volume by the operator before the run (see cloud_program/README.md).
Why upload is the correct route for this arm: the negfam generator
(code/negfam/gen_negfam.py) is NOT reproducible from the pod gen path
(pod/gen_on_pod.py has no negfam branch). Its WIDE-row construction is
a separate draw sequence (key counts, then cat/plen/pcode draws in an
order the pod generator never performs) PLUS a DuckDB materialisation
step (repeat('w'/'n', plen) string construction + ZSTD parquet write)
that has no Spark equivalent in this package. Re-implementing it on
Databricks would require porting the DuckDB string materialisation to
Spark and re-validating byte-exact equality against the local files --
work that buys nothing, because the validated local parquet
(data/negfam_s101/, validation.json all diffs 0, 89 MB total) already
exists and uploads in seconds. Upload preserves bit-identity; the
script verifies the upload by re-deriving row counts, total payload
bytes, and max per-key row counts and comparing them against the
local gt_negfam.json values when --gt-json is given (reported, not
asserted -- a mismatch is a finding, recorded in the manifest).

COMPARISON: synopsis-arm predictions from predictions_negfam.csv
(--predictions, uploaded by the operator) vs the Spark verdicts.
Default arms: A5-NF, A8-NF ONLY (PROGRAM_DESIGN.md S.4.4: the sampling
arms' negfam behaviour stays a local result unless the execution phase
extends the arm by design amendment). The script REPORTS what it
measures (per-arm agreement, recall with FAIL as the positive class,
TP/FP/FN/TN); it does NOT assert the pre-registered expectation
(synopsis recall 0.000). Any outcome -- including a synopsis prediction
unexpectedly matching -- is reported as measured.

Fact schema (from code/negfam/gen_negfam.py): rid INT, k INT,
cat INT (1 = WIDE), payload STRING, sprefix STRING. dim: dim_key INT,
dpayload STRING (48 chars).

Usage (as a Databricks spark_python_task; see job_defs/d_neg.json):
  negfam_dbx.py --data /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/negfam_s101
      --predictions /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/negfam_s101/predictions_negfam.csv
      --manifest /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/negfam_s101/negfam_manifest.json
      [--gt-json <volume path to gt_negfam.json>]
"""
import argparse, csv, json, os, time

P_PARTITIONS = 128
PIPES = ["V1", "V2", "V3", "V4"]
THRESH = {"V1": 4_000_000.0, "V2": 10_000_000.0,
          "V3": 50_000_000.0, "V4": 50_000_000_000.0}
DEFAULT_ARMS = ["A5-NF", "A8-NF"]


def partition_bytes(df, byte_col):
    """D8 byte measurement: per-partition SUM of a byte column.

    Returns {"max_partition_bytes", "partition_byte_vector" (128 ints),
    "n_nonempty_partitions", "total_bytes"}.
    """
    from pyspark.sql import functions as F
    rows = (df.groupBy(F.spark_partition_id().alias("pid"))
            .agg(F.sum(byte_col).alias("b")).collect())
    vec = [0] * P_PARTITIONS
    for r in rows:
        vec[int(r["pid"])] = int(r["b"] or 0)
    return {"max_partition_bytes": int(max(vec)),
            "partition_byte_vector": vec,
            "n_nonempty_partitions": int(sum(1 for v in vec if v > 0)),
            "total_bytes": int(sum(vec))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True,
                    help="volume dir with negfam fact.parquet/dim.parquet")
    ap.add_argument("--predictions", required=True,
                    help="volume path to predictions_negfam.csv (uploaded)")
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--gt-json", default=None,
                    help="volume path to gt_negfam.json (optional; enables "
                         "the Spark-vs-local-GT cross-check)")
    ap.add_argument("--arms", default=",".join(DEFAULT_ARMS),
                    help="arms to compare (default: synopsis arms only, "
                         "A5-NF,A8-NF -- see header)")
    ap.add_argument("--gen-mode", default="upload", choices=["upload"],
                    help="only 'upload' is supported; the negfam generator "
                         "is not reproducible from the pod gen path (see header)")
    ap.add_argument("--labelling", default="Databricks serverless Jobs compute "
                    "-- D-NEG negative-family confirmation; see manifest")
    args = ap.parse_args()

    from pyspark.sql import SparkSession, functions as F

    spark = (SparkSession.builder.appName("BlindAtScale-NegFam-Databricks")
             .config("spark.sql.shuffle.partitions", str(P_PARTITIONS))
             .config("spark.sql.files.maxPartitionBytes", "8388608")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.databricks.photon.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    print(f"spark={spark.version} app_id={spark.sparkContext.applicationId}")

    fact = spark.read.parquet(os.path.join(args.data, "fact.parquet"))
    dim = spark.read.parquet(os.path.join(args.data, "dim.parquet"))
    n_fact = fact.count()
    print(f"n_fact={n_fact}")

    local_gt = None
    if args.gt_json and os.path.exists(args.gt_json):
        with open(args.gt_json) as f:
            local_gt = json.load(f)

    manifest = {"spark_version": spark.version, "n_fact": n_fact,
                "shuffle_partitions": P_PARTITIONS,
                "thresholds": THRESH,
                "gen_mode": args.gen_mode,
                "labelling": args.labelling,
                "measurement": {
                    "verdict_bearing_source":
                        "executed Spark byte loads (D8, byte aggregation); "
                        "V1 per-key max, V2 per-partition join-output bytes, "
                        "V3 per-sprefix max, V4 per-partition cost. "
                        "See header for the per-pipeline definition match.",
                    "load_semantics": ("BYTE loads (sums of LENGTH(payload)), "
                                       "NOT row counts. Do not compare against "
                                       "row-count thresholds or loads."),
                    "comparison_note": ("The script REPORTS measured agreement; "
                                        "it does not assert the pre-registered "
                                        "expectation (synopsis recall 0.000).")},
                "pipelines": []}
    if os.path.exists(args.manifest):
        os.remove(args.manifest)

    # Upload verification: re-derive dataset-level quantities.
    tot_bytes = fact.agg(F.sum(F.length("payload")).alias("b")).collect()[0]["b"]
    max_key_rows = (fact.groupBy("k").agg(F.count("*").alias("c"))
                    .agg(F.max("c")).collect()[0][0])
    manifest["upload_verification"] = {
        "n_fact": int(n_fact), "total_payload_bytes": int(tot_bytes),
        "max_key_row_count": int(max_key_rows)}
    if local_gt:
        manifest["upload_verification"]["local_total_payload_bytes"] = \
            local_gt.get("total_payload_bytes")
        manifest["upload_verification"]["local_max_key_row_count"] = \
            local_gt.get("max_key_row_count")
        manifest["upload_verification"]["total_bytes_match"] = bool(
            int(tot_bytes) == int(local_gt.get("total_payload_bytes", -1)))
        manifest["upload_verification"]["max_key_rows_match"] = bool(
            int(max_key_rows) == int(local_gt.get("max_key_row_count", -1)))
    print(f"upload verification: {manifest['upload_verification']}")

    def record(pid, fn):
        t0 = int(time.time() * 1000)
        entry = {"pipeline": pid, "start_ms": t0, "threshold": THRESH[pid]}
        try:
            info = fn()
            entry.update(info)
            entry["status"] = entry.get("status", "OK")
        except Exception as e:  # keep going: one failure must not lose the rest
            entry["status"] = "ERROR"
            entry["error"] = repr(e)[:500]
            print(f"[{pid}] ERROR {e!r}")
        entry["end_ms"] = int(time.time() * 1000)
        entry["wall_seconds"] = round((entry["end_ms"] - t0) / 1000.0, 2)
        if entry.get("gt_load") is not None:
            entry["verdict"] = ("FAIL" if entry["gt_load"] > THRESH[pid]
                                else "PASS")
        if local_gt and entry.get("gt_load") is not None:
            lg = local_gt["gt_loads"].get(pid)
            entry["local_gt_load"] = lg
            entry["ratio_spark_over_local_gt"] = (
                entry["gt_load"] / lg if lg else None)
            entry["local_gt_verdict"] = local_gt["gt_verdicts"].get(pid)
        manifest["pipelines"].append(entry)
        with open(args.manifest, "w") as fh:
            json.dump(manifest, fh, indent=1)
        print(f"[{pid}] {entry['status']} gt_load={entry.get('gt_load')} "
              f"verdict={entry.get('verdict')} wall={entry['wall_seconds']}s")
        spark.catalog.clearCache()

    def do_v1():
        per_key = (fact.groupBy("k")
                   .agg(F.sum(F.length("payload")).alias("b")))
        rows = per_key.collect()
        gt = float(max(r["b"] for r in rows))
        rep = fact.repartition(P_PARTITIONS, "k")
        pb = partition_bytes(rep, F.length("payload"))
        return {"gt_load": gt, "gt_load_source": "executed per-key max bytes (V1 definition)",
                "per_partition_bytes": pb}

    def do_v2():
        joined = fact.join(dim.hint("SHUFFLE_MERGE"),
                           fact["k"] == dim["dim_key"], "inner")
        jb = joined.withColumn("jb", F.length("payload") + F.length("dpayload"))
        rep = jb.repartition(P_PARTITIONS, "k")
        pb = partition_bytes(rep, "jb")
        return {"gt_load": float(pb["max_partition_bytes"]),
                "gt_load_source": ("per-partition max of join-output bytes "
                                   "(V2 definition, murmur3/D1)"),
                "per_partition_bytes": pb,
                "join_output_rows": int(jb.count())}

    def do_v3():
        per_sp = (fact.groupBy("sprefix")
                  .agg(F.sum(F.length("payload")).alias("b")))
        rows = per_sp.collect()
        mx = max(rows, key=lambda r: r["b"])
        rep = fact.repartition(P_PARTITIONS, "sprefix")
        pb = partition_bytes(rep, F.length("payload"))
        return {"gt_load": float(mx["b"]),
                "gt_load_source": "executed per-sprefix max bytes (V3 definition)",
                "argmax_sprefix": str(mx["sprefix"]),
                "per_partition_bytes": pb}

    def do_v4():
        per_key = (fact.groupBy("k").agg(
            F.sum(F.length("payload").cast("bigint")
                  * F.length("payload").cast("bigint")).alias("cost")))
        rep = per_key.repartition(P_PARTITIONS, "k")
        pb = partition_bytes(rep, "cost")
        return {"gt_load": float(pb["max_partition_bytes"]),
                "gt_load_source": ("per-partition max of per-key "
                                   "SUM(LENGTH^2) (V4 definition, murmur3/D1)"),
                "per_partition_cost": pb}

    impls = {"V1": do_v1, "V2": do_v2, "V3": do_v3, "V4": do_v4}
    for pid in PIPES:
        print(f"=== {pid} (threshold {THRESH[pid]:,.0f} B) ===")
        record(pid, impls[pid])

    # --- comparison vs synopsis-arm predictions (reported, not asserted) ---
    spark_verdicts = {e["pipeline"]: e.get("verdict")
                      for e in manifest["pipelines"]}
    arms = [a.strip() for a in args.arms.split(",") if a.strip()]
    preds = {}
    with open(args.predictions, newline="") as f:
        for row in csv.DictReader(f):
            if row["arm"] in arms and row["pipeline"] in PIPES:
                preds.setdefault(row["arm"], {})[row["pipeline"]] = {
                    "pred_load": float(row["pred_load"]),
                    "pred_verdict": row["pred_verdict"]}
    comparison = {}
    for arm in arms:
        tp = fp = fn = tn = agree = 0
        per = {}
        for pid in PIPES:
            p = preds.get(arm, {}).get(pid)
            sv = spark_verdicts.get(pid)
            if p is None or sv is None:
                per[pid] = {"pred_verdict": p["pred_verdict"] if p else None,
                            "spark_verdict": sv, "compared": False}
                continue
            pv = p["pred_verdict"]
            agree += int(pv == sv)
            if pv == "FAIL" and sv == "FAIL":
                tp += 1
            elif pv == "FAIL" and sv == "PASS":
                fp += 1
            elif pv == "PASS" and sv == "FAIL":
                fn += 1
            else:
                tn += 1
            per[pid] = {"pred_load": p["pred_load"], "pred_verdict": pv,
                        "spark_verdict": sv, "verdict_agrees": pv == sv,
                        "compared": True}
        comparison[arm] = {
            "verdict_agreement": f"{agree}/4",
            "recall_fail_positive": (tp / (tp + fn) if (tp + fn) else None),
            "precision_fail_positive": (tp / (tp + fp) if (tp + fp) else None),
            "tp_fp_fn_tn": [tp, fp, fn, tn],
            "per_pipeline": per}
    manifest["comparison_vs_predictions"] = comparison
    manifest["comparison_note"] = ("Reported as measured. The pre-registered "
                                   "expectation (synopsis recall 0.000) is NOT "
                                   "asserted by this script.")
    with open(args.manifest, "w") as fh:
        json.dump(manifest, fh, indent=1)
    spark.stop()
    print(f"comparison: {json.dumps({a: c['verdict_agreement'] for a, c in comparison.items()})}")
    print(f"manifest -> {args.manifest}")


if __name__ == "__main__":
    main()
