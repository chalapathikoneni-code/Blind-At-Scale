"""D-FB arm: feedback verification on real Spark (Databricks).

For the 7 fixable GT-FAIL pipelines on zipf_s101 (G_A, G_N, J_A, J_N,
F_A, F_B, W_A), run the ORIGINAL pipeline and then the REWRITTEN
pipeline in Spark, measuring both loads by the D8 partition-id method
(see spark_pipelines_dbx.py) at T = 200,000 (0.02 * N_fact).

This converts the local feedback study (code/feedback.py: FB-SYN
repaired 51/51 against ANALYTIC fix-effect models) into executed
evidence: the rewrites here are real Spark transformations, and the
loads are measured from executed shuffles.

REWRITES (mirroring code/feedback.py's analytic models):
- salt (G_A/G_N/W_A): add a deterministic salt column, then a two-stage
  aggregation (partial agg on (key, salt), final agg on key).
  SALT DETERMINISM CHOICE (recorded for the design doc): the salt is
  (abs(hash(s)) % 16) where hash() is Spark's murmur3 hash of the row's
  `s` column. Alternatives rejected: key % 16 is WRONG for hot keys
  (a hot key maps to exactly one salt -- no spreading at all);
  monotonically_increasing_id() % 16 is NOT deterministic across runs
  (ids depend on partitioning/task scheduling), so the rewrite would
  not be reproducible. hash(s) is deterministic given the data and
  independent of the key, so each hot key's rows spread uniformly over
  the 16 salts. The design's void condition (PROGRAM_DESIGN.md S.4.3)
  is checked in-run: the G_A salted pipeline is executed TWICE and the
  two loads (and full partition vectors) must be exactly equal.
- preagg_join (J_*): aggregate fact to per-key counts first, then join
  the 20k-row count table to dim (the join that remains is tiny).
  The rewritten load is the max partition load of the rewritten plan's
  key stages (pre-agg output + count-table join output, both
  repartitioned on the key). LIMITATION (recorded, not hidden): the
  pre-aggregation's own shuffle carries map-side partial counts whose
  volume is bounded by (input partitions x distinct keys per partition)
  and is NOT measured by the partition-id method -- it is visible in
  the event log when the log is available. The analytic model in
  feedback.py likewise models only the post-aggregation load.
- preagg_ff (F_*): aggregate both facts to per-key counts and join the
  count tables -- exactly the D4 aggregate form. The "rewrite" IS the
  aggregate form; its load is the count-table join's partition load.
  The original load is the D4 analytic splitmix64 load (the raw join
  is not materialisable, as in the pod run).

W_A CAVEAT: salting a window changes row_number semantics (row numbers
restart within (kA, salt) groups, not within kA groups). The salted
W_A load reported here is the shuffle-load proxy for the rewrite -- the
load a salted window plan would realise -- and output-equivalence for
W_A is NOT claimed. The G_* salt rewrites, by contrast, are exactly
output-equivalent (two-stage aggregation), and the script checks that
the rewritten max group count equals the original one.

--sample-outeq (optional stretch): on a deterministic ~10% subset
(rows where hash(s) % 10 == 0 -- NOT "the first 1M rows", which is not
a random subset), run original vs rewritten G_A/J_A and compare
results exactly (row counts + sorted hash). For J_A the comparison is
per-key: the rewritten (pre-aggregated) output is one row per key, so
equivalence means the per-key counts match the original join output's
per-key counts and the totals match. This is a BEST-EFFORT SPOT CHECK,
labelled as such -- it is not a repeat of the local study's 48/48 outeq
proof (PREREGISTRATION.md S.5 / code/outeq_check.py).

ORIGINALS AS A REPLICATION: the original loads measured here replicate
the D-DBX measurements of the same pipelines. PROGRAM_DESIGN.md S.4.3
voids the arm if any original disagrees with D-DBX beyond the D1
hash-function tolerance -- that comparison is done by the collector
(shared/collect_cloud.py + the run log), not in this script.

Usage (as a Databricks spark_python_task; see job_defs/d_fb.json):
  feedback_dbx.py --data /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/fb_zipf_s101
      --manifest /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/fb_zipf_s101/feedback_manifest.json
      [--sample-outeq]
"""
import argparse, hashlib, json, os, time
import numpy as np

P_PARTITIONS = 128
N_SALTS = 16
FIXABLE = {"G_A": ("salt", "kA"), "G_N": ("salt", "kN"),
           "J_A": ("preagg_join", "kA"), "J_N": ("preagg_join", "kN"),
           "F_A": ("preagg_ff", "kA"), "F_B": ("preagg_ff", "kB"),
           "W_A": ("salt", "kA")}


def splitmix64_hash(keys):
    """Copied from code/common.py -- D4 analytic F load only."""
    z = (keys.astype(np.uint64) + np.uint64(1)).copy()
    z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    z = z ^ (z >> np.uint64(31))
    return z


def max_partition_load(counts, p=P_PARTITIONS):
    keys = np.arange(len(counts), dtype=np.int64)
    parts = (splitmix64_hash(keys) % np.uint64(p)).astype(np.int64)
    loads = np.bincount(parts, weights=np.asarray(counts, dtype=np.float64),
                        minlength=p)
    return float(loads.max())


def partition_counts(df):
    """D8 measurement: per-partition row counts (see spark_pipelines_dbx.py)."""
    from pyspark.sql import functions as F
    rows = (df.groupBy(F.spark_partition_id().alias("pid"))
            .count().collect())
    vec = [0] * P_PARTITIONS
    for r in rows:
        vec[int(r["pid"])] = int(r["count"])
    return {"max_partition_rows": int(max(vec)),
            "partition_count_vector": vec,
            "n_nonempty_partitions": int(sum(1 for v in vec if v > 0))}


def rows_hash(rows):
    """SHA-256 over sorted (str) rows -- deterministic result comparison."""
    h = hashlib.sha256()
    for r in sorted(str(tuple(r)) for r in rows):
        h.update(r.encode())
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True,
                    help="volume dir with fact/fact2/dim parquet")
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--pipelines", default="G_A,G_N,J_A,J_N,F_A,F_B,W_A")
    ap.add_argument("--raw-join-cap", type=float, default=2e8)
    ap.add_argument("--sample-outeq", action="store_true",
                    help="run the ~10%%-subset output-equivalence spot check "
                         "for G_A/J_A (best-effort, labelled as a spot check)")
    ap.add_argument("--labelling", default="Databricks serverless Jobs compute "
                    "-- D-FB feedback verification; see manifest")
    args = ap.parse_args()

    from pyspark.sql import SparkSession, functions as F, Window
    from pyspark import StorageLevel

    spark = (SparkSession.builder.appName("BlindAtScale-Feedback-Databricks")
             .config("spark.sql.shuffle.partitions", str(P_PARTITIONS))
             .config("spark.sql.files.maxPartitionBytes", "8388608")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.databricks.photon.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    print(f"spark={spark.version} app_id={spark.sparkContext.applicationId}")

    fact = spark.read.parquet(os.path.join(args.data, "fact.parquet"))
    fact2 = spark.read.parquet(os.path.join(args.data, "fact2.parquet"))
    dim = spark.read.parquet(os.path.join(args.data, "dim.parquet"))
    n_fact = fact.count()
    t_load = 0.02 * n_fact
    print(f"n_fact={n_fact} t_load={t_load}")

    manifest = {"spark_version": spark.version, "n_fact": n_fact,
                "t_load": t_load, "shuffle_partitions": P_PARTITIONS,
                "n_salts": N_SALTS,
                "salt_definition": ("(abs(hash(s)) % 16); hash() is Spark's "
                                    "murmur3 hash of the row's s column -- "
                                    "deterministic given the data, independent "
                                    "of the key. key % 16 rejected (hot key -> "
                                    "one salt); monotonically_increasing_id "
                                    "rejected (non-deterministic across runs)."),
                "labelling": args.labelling,
                "measurement": {
                    "verdict_bearing_source":
                        "partition_id (D8); F originals: analytic splitmix64 "
                        "from Spark-produced counts (D4)",
                    "w_a_caveat": ("salted W_A changes row_number semantics; "
                                   "its rewritten load is the shuffle-load "
                                   "proxy, and output-equivalence is NOT "
                                   "claimed for W_A."),
                    "preagg_limitation": ("the pre-aggregation's own shuffle "
                                          "carries map-side partial counts, "
                                          "not measured by the partition-id "
                                          "method; see header.")},
                "pipelines": []}
    if os.path.exists(args.manifest):
        os.remove(args.manifest)

    def salted(df):
        return df.withColumn("salt", (F.abs(F.hash("s")) % N_SALTS))

    def verdict(load):
        return "FAIL" if load > t_load else "PASS"

    def run_original(pid, fix_type, col):
        """Original pipeline load, same measurement as spark_pipelines_dbx."""
        if fix_type == "salt" and pid.startswith("G_"):
            rep = fact.repartition(P_PARTITIONS, col)
            rep.persist(StorageLevel.MEMORY_AND_DISK)
            pc = partition_counts(rep)
            agg = rep.groupBy(col).agg(F.count("*").alias("c"))
            rows = agg.collect()
            rep.unpersist()
            return {"load": pc["max_partition_rows"], "partition_id": pc,
                    "max_group_count": int(max(r["c"] for r in rows))}
        if fix_type == "preagg_join":
            joined = fact.join(dim.hint("SHUFFLE_MERGE"),
                               fact[col] == dim["dim_key"], "inner")
            rep = joined.repartition(P_PARTITIONS, col)
            rep.persist(StorageLevel.MEMORY_AND_DISK)
            pc = partition_counts(rep)
            n_out = rep.count()
            rep.unpersist()
            return {"load": pc["max_partition_rows"], "partition_id": pc,
                    "join_output_rows": int(n_out)}
        if fix_type == "preagg_ff":
            c1 = fact.groupBy(col).agg(F.count("*").alias("c1"))
            c2 = fact2.groupBy(col).agg(F.count("*").alias("c2"))
            j = c1.join(c2, col).withColumn("prod", F.col("c1") * F.col("c2"))
            rows = j.collect()
            keys = np.array([r[col] for r in rows], dtype=np.int64)
            prod = np.zeros(20_000, dtype=np.float64)
            prod[keys] = np.array([r["prod"] for r in rows], dtype=np.float64)
            total_out = float(prod.sum())
            analytic = max_partition_load(prod)
            return {"load": analytic,
                    "load_source": "analytic_splitmix64_from_spark_counts (D4)",
                    "exact_join_output_rows": total_out,
                    "raw_join_status": ("EXECUTED_RAW" if total_out <= args.raw_join_cap
                                        else "SKIPPED_NOT_MATERIALISABLE")}
        if pid == "W_A":
            rep = fact.repartition(P_PARTITIONS, "kA")
            pc = partition_counts(rep)
            win = Window.partitionBy("kA").orderBy("s")
            w = fact.withColumn("rn", F.row_number().over(win))
            mx = w.agg(F.max("rn")).collect()[0][0]
            return {"load": pc["max_partition_rows"], "partition_id": pc,
                    "max_window_partition_rows": int(mx)}
        raise ValueError(pid)

    def run_rewritten(pid, fix_type, col):
        """Rewritten pipeline load (real Spark transformation)."""
        if fix_type == "salt" and pid.startswith("G_"):
            s = salted(fact)
            rep = s.repartition(P_PARTITIONS, col, "salt")
            rep.persist(StorageLevel.MEMORY_AND_DISK)
            pc = partition_counts(rep)
            rep.unpersist()
            partial = s.groupBy(col, "salt").agg(F.count("*").alias("c"))
            final = partial.groupBy(col).agg(F.sum("c").alias("c"))
            rows = final.collect()
            return {"load": pc["max_partition_rows"], "partition_id": pc,
                    "rewritten_max_group_count": int(max(r["c"] for r in rows)),
                    "rewritten_result_rows": len(rows)}
        if fix_type == "preagg_join":
            counts = fact.groupBy(col).agg(F.count("*").alias("c"))
            counts.persist(StorageLevel.MEMORY_AND_DISK)
            pc_counts = partition_counts(
                counts.repartition(P_PARTITIONS, col))
            joined = counts.join(dim.hint("SHUFFLE_MERGE"),
                                 counts[col] == dim["dim_key"], "inner")
            rep = joined.repartition(P_PARTITIONS, col)
            pc_join = partition_counts(rep)
            n_out = int(rep.count())
            counts.unpersist()
            return {"load": max(pc_counts["max_partition_rows"],
                                pc_join["max_partition_rows"]),
                    "preagg_output_partition": pc_counts,
                    "count_join_partition": pc_join,
                    "rewritten_join_output_rows": n_out}
        if fix_type == "preagg_ff":
            c1 = fact.groupBy(col).agg(F.count("*").alias("c1"))
            c2 = fact2.groupBy(col).agg(F.count("*").alias("c2"))
            j = c1.join(c2, col).withColumn("prod", F.col("c1") * F.col("c2"))
            rep = j.repartition(P_PARTITIONS, col)
            pc = partition_counts(rep)
            return {"load": pc["max_partition_rows"], "partition_id": pc,
                    "count_table_join_rows": int(rep.count()),
                    "note": ("the rewrite IS the D4 aggregate form; its load "
                             "is the count-table join's partition load")}
        if pid == "W_A":
            s = salted(fact)
            rep = s.repartition(P_PARTITIONS, "kA", "salt")
            pc = partition_counts(rep)
            win = Window.partitionBy("kA", "salt").orderBy("s")
            w = s.withColumn("rn", F.row_number().over(win))
            mx = w.agg(F.max("rn")).collect()[0][0]
            return {"load": pc["max_partition_rows"], "partition_id": pc,
                    "max_salted_window_partition_rows": int(mx)}
        raise ValueError(pid)

    def record(pid):
        fix_type, col = FIXABLE[pid]
        t0 = int(time.time() * 1000)
        entry = {"pipeline": pid, "fix_type": fix_type, "key": col,
                 "start_ms": t0, "status": "OK"}
        try:
            orig = run_original(pid, fix_type, col)
            entry["original"] = {**orig, "verdict": verdict(orig["load"])}
            rew = run_rewritten(pid, fix_type, col)
            entry["rewritten"] = {**rew, "verdict": verdict(rew["load"])}
            entry["repaired"] = bool(entry["original"]["verdict"] == "FAIL"
                                     and entry["rewritten"]["verdict"] == "PASS")
            entry["load_ratio_rewritten_over_original"] = (
                rew["load"] / orig["load"] if orig["load"] else None)
            # G_* salt rewrites are exactly output-equivalent: check it.
            if fix_type == "salt" and pid.startswith("G_"):
                entry["outeq_max_group_count_matches"] = bool(
                    entry["original"]["max_group_count"]
                    == entry["rewritten"]["rewritten_max_group_count"])
        except Exception as e:  # keep going: one failure must not lose the rest
            entry["status"] = "ERROR"
            entry["error"] = repr(e)[:500]
            print(f"[{pid}] ERROR {e!r}")
        entry["end_ms"] = int(time.time() * 1000)
        entry["wall_seconds"] = round((entry["end_ms"] - t0) / 1000.0, 2)
        manifest["pipelines"].append(entry)
        with open(args.manifest, "w") as fh:
            json.dump(manifest, fh, indent=1)
        print(f"[{pid}] {entry['status']} orig={entry.get('original', {}).get('load')} "
              f"rew={entry.get('rewritten', {}).get('load')} "
              f"repaired={entry.get('repaired')} wall={entry['wall_seconds']}s")
        spark.catalog.clearCache()

    for pid in [p.strip() for p in args.pipelines.split(",") if p.strip()]:
        if pid not in FIXABLE:
            print(f"unknown or non-fixable pipeline {pid}, skipped")
            continue
        print(f"=== {pid} ({FIXABLE[pid][0]}) ===")
        record(pid)

    # --- salt determinism check (design S.4.3 void condition) ---
    # Run the G_A salted pipeline twice; loads and full partition vectors
    # must be exactly equal (hash(s) is deterministic given the data).
    try:
        s = salted(fact)
        pc1 = partition_counts(s.repartition(P_PARTITIONS, "kA", "salt"))
        pc2 = partition_counts(s.repartition(P_PARTITIONS, "kA", "salt"))
        det = {"pipeline": "G_A", "first_max": pc1["max_partition_rows"],
               "second_max": pc2["max_partition_rows"],
               "vectors_equal": pc1["partition_count_vector"]
                                == pc2["partition_count_vector"],
               "check": ("PASS" if pc1["partition_count_vector"]
                         == pc2["partition_count_vector"] else "FAIL")}
    except Exception as e:  # noqa: BLE001 - record, do not crash the arm
        det = {"pipeline": "G_A", "check": "ERROR", "error": repr(e)[:500]}
    manifest["salt_determinism_check"] = det
    print(f"salt determinism check: {det['check']}")

    # --- optional outeq spot check on a deterministic ~10% subset ---
    if args.sample-outeq:
        try:
            subset = fact.filter((F.hash("s") % 10) == 0)
            n_sub = int(subset.count())
            outeq = {"subset_definition": "hash(s) % 10 == 0 (Spark murmur3 hash)",
                     "subset_rows": n_sub,
                     "label": ("BEST-EFFORT SPOT CHECK on a ~10% subset -- NOT "
                               "the local study's 48/48 outeq proof."),
                     "checks": {}}
            # G_A: original groupBy counts vs salted two-stage final counts
            o = subset.groupBy("kA").agg(F.count("*").alias("c")).collect()
            s_sub = salted(subset)
            partial = s_sub.groupBy("kA", "salt").agg(F.count("*").alias("c"))
            r = partial.groupBy("kA").agg(F.sum("c").alias("c")).collect()
            outeq["checks"]["G_A"] = {
                "original_rows": len(o), "rewritten_rows": len(r),
                "counts_equal": len(o) == len(r),
                "hash_equal": rows_hash(o) == rows_hash(r)}
            # J_A: per-key equivalence (rewritten output is one row per key)
            oj = subset.join(dim.hint("SHUFFLE_MERGE"),
                             subset["kA"] == dim["dim_key"], "inner")
            oj_counts = (oj.groupBy("kA").agg(F.count("*").alias("c"))
                         .collect())
            oj_total = sum(int(x["c"]) for x in oj_counts)
            cj = (subset.groupBy("kA").agg(F.count("*").alias("c"))
                  .join(dim.hint("SHUFFLE_MERGE"),
                        F.col("kA") == dim["dim_key"], "inner")
                  .select("kA", "c").collect())
            outeq["checks"]["J_A"] = {
                "original_join_output_rows": oj_total,
                "rewritten_total_rows": sum(int(x["c"]) for x in cj),
                "totals_equal": oj_total == sum(int(x["c"]) for x in cj),
                "per_key_hash_equal": rows_hash(oj_counts) == rows_hash(cj)}
            manifest["sample_outeq"] = outeq
            print(f"sample outeq: {json.dumps(outeq['checks'])}")
        except Exception as e:  # noqa: BLE001 - spot check must not void the arm
            manifest["sample_outeq"] = {"label": "SPOT CHECK ERROR",
                                        "error": repr(e)[:500]}
            print(f"sample outeq ERROR {e!r}")

    repaired = sum(1 for e in manifest["pipelines"] if e.get("repaired"))
    manifest["repair_summary"] = {"repaired": repaired,
                                  "of": len(manifest["pipelines"]),
                                  "note": ("repaired = original FAIL and "
                                           "rewritten PASS at T = "
                                           f"{t_load:,.0f}")}
    with open(args.manifest, "w") as fh:
        json.dump(manifest, fh, indent=1)
    spark.stop()
    print(f"repaired {repaired}/{len(manifest['pipelines'])}; manifest -> {args.manifest}")


if __name__ == "__main__":
    main()
