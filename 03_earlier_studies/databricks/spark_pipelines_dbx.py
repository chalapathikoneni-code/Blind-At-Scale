"""Spark implementations of the Blind at Scale headline pipelines, executed
for REAL on Databricks (serverless Jobs compute by default), with
ground-truth loads measured DIRECTLY per partition (deviation D8).

This is an adaptation of pod/spark_pipelines.py. Pipelines, thresholds,
and deviations D1-D7 are unchanged from the pod script -- read its header
for the full definitions. What changes here is ONLY the measurement path
(D8) plus the Databricks execution environment.

Pipelines (PREREGISTRATION.md S.2):
  G_A, G_B, G_N   GROUP BY on fact key kA / kB / kN
  J_A, J_B, J_N   fact JOIN dim on kA / kB / kN
  F_A, F_B        fact JOIN fact2 on kA / kB   (guarded -- see D4 below)
  W_A             window partitioned by kA
All run with spark.sql.shuffle.partitions = 128 (matching P = 128 locally).

=========================================================================
DEVIATION D8 -- DIRECT PER-PARTITION MEASUREMENT (new in this adaptation)
=========================================================================
The pod run measured loads from the Spark event log (max per-task
shuffle-read records). On serverless Jobs compute the event log may not
be downloadable, so this script ALSO measures each pipeline's load
directly: after the pipeline's shuffle/repartition, it computes
per-partition row counts via spark_partition_id()
(groupBy spark_partition_id().count()) and records max_partition_rows
plus the full 128-element partition-count vector.

- G_*: measured after the explicit repartition(128, col) -- this is
  exactly the quantity the event log's shuffle-read measures (raw fact
  rows in the busiest murmur3 partition), realised by an executed
  shuffle. The repartitioned DataFrame is persisted so the partition
  measurement and the aggregation read the SAME materialised partitions.
- J_*: measured on the join output repartitioned on the join key
  (repartition(128, col) then partition counts) -- the local definition
  (max partition of join-output rows). Because dim holds every key
  exactly once, join-output rows per partition = fact rows per partition
  (except J_N, where kN = -1 rows drop out -- mirroring the local J_N
  masking of the NULL group).
- W_A: measured on fact repartitioned by kA. The window itself runs on
  fact exactly as in the pod script; the window's own shuffle uses the
  same murmur3/128 partitioner as the measured repartition, so the
  measured vector is the window's partition load.
- F_*: D4 unchanged (aggregate form + analytic splitmix64 load from
  Spark-produced counts; raw join only if the exact output is <= 2e8
  rows). The partition-id method does not apply to a join whose output
  is not materialisable; the verdict-bearing load is ALWAYS the
  analytic splitmix64 load, exactly as in the pod run.

VERDICT-BEARING SOURCE ON SERVERLESS: the D8 partition-id measurement
(F: the D4 analytic load). The event-log path is KEPT as best-effort
corroboration: event logging is enabled, the log dir is recorded, and
each pipeline's [start_ms, end_ms] window is recorded so a post-hoc
parse (pod/collect_results.py style) can attribute tasks IF the log is
downloadable from the run. In-job, event-log-derived fields are NOT
available (the log is written as the job runs); the per-pipeline
"event_log" block therefore records availability + the window, not
task metrics. Where both measurements exist, a material disagreement
is a finding, reported, not averaged away.

TOPOLOGY PROOF: the driver records executor hostnames from
sc.statusTracker().getExecutorInfos() (plus a getExecutorMemoryStatus
fallback) in the manifest: distinct executor host count, executor IDs.
The D-DBX success criterion (PROGRAM_DESIGN.md S.4.1) is >= 2 distinct
executor hosts. If host information is unavailable or degenerate on
serverless, the arm's topology claim is VOID (not fudged).

DEVIATION D9 (Photon): Photon is forced OFF
(spark.databricks.photon.enabled=false) to match the pod run's vanilla
Spark 3.5.3 semantics. The script reads back the effective value; if
serverless forces Photon on, the manifest records deviation D9 FIRED
and the affected loads are labelled Photon-executed.

Other deviations D1-D7: as in pod/spark_pipelines.py (murmur3 vs
splitmix64; G/W repartition upper bound; J shuffle-merge, broadcast
off; F not materialisable; scan splits bounded by maxPartitionBytes
8 MB; AQE off).

Usage (as a Databricks spark_python_task; see job_defs/):
  spark_pipelines_dbx.py --data /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/zipf_s101
      --manifest /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/zipf_s101/run_manifest.json
      [--pipelines G_A,G_B,...] [--sample-out <volume path for 1% sample>]
"""
import argparse, json, os, time
import numpy as np

P_PARTITIONS = 128
T_LOAD_HEADLINE = 200_000


def splitmix64_hash(keys):
    """Copied from code/common.py (local study) -- used ONLY for the
    clearly-labelled analytic F cross-check (deviation D4)."""
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


def partition_counts(df, spark):
    """D8 measurement: per-partition row counts of a DataFrame.

    Returns {"max_partition_rows", "partition_count_vector" (128 ints,
    zero-padded for empty partitions), "n_nonempty_partitions"}.
    """
    from pyspark.sql import functions as F
    rows = (df.groupBy(F.spark_partition_id().alias("pid"))
            .count().collect())
    vec = [0] * P_PARTITIONS
    for r in rows:
        vec[int(r["pid"])] = int(r["count"])
    return {"max_partition_rows": int(max(vec)),
            "partition_count_vector": vec,
            "n_nonempty_partitions": int(sum(1 for v in vec if v > 0))}


def executor_topology(spark):
    """Best-effort executor hostname proof (D-DBX topology criterion).

    Never raises: on any failure records available=false with the error.
    """
    topo = {"available": False, "n_executors": 0, "distinct_hosts": 0,
            "hosts": [], "executor_ids": [], "source": None, "error": None}
    try:
        infos = spark.sparkContext.statusTracker().getExecutorInfos()
        hosts, ids = [], []
        for info in infos:
            h = getattr(info, "host", None)
            if callable(h):
                h = h()
            hosts.append(h)
            eid = getattr(info, "executorId", None)
            if callable(eid):
                eid = eid()
            ids.append(eid)
        topo.update({"available": True, "n_executors": len(infos),
                     "distinct_hosts": len({h for h in hosts if h}),
                     "hosts": hosts, "executor_ids": ids,
                     "source": "statusTracker.getExecutorInfos"})
        return topo
    except Exception as e:  # noqa: BLE001 - best-effort probe
        topo["error"] = f"statusTracker: {e!r}"[:300]
    try:  # fallback: BlockManagerIds from getExecutorMemoryStatus
        mem = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
        hosts = sorted({str(bm.host()) for bm in mem.keySet()})
        topo.update({"available": True, "n_executors": len(hosts),
                     "distinct_hosts": len(hosts), "hosts": hosts,
                     "source": "getExecutorMemoryStatus fallback"})
    except Exception as e:  # noqa: BLE001 - best-effort probe
        topo["error"] = (topo.get("error") or "") + f" | memStatus: {e!r}"[:300]
    return topo


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True,
                    help="volume dir with fact/fact2/dim parquet")
    ap.add_argument("--manifest", required=True, help="volume path for the manifest JSON")
    ap.add_argument("--pipelines", default="G_A,G_B,G_N,J_A,J_B,J_N,F_A,F_B,W_A")
    ap.add_argument("--raw-join-cap", type=float, default=2e8)
    ap.add_argument("--eventlog-dir", default=None,
                    help="default: <manifest dir>/eventlog (best-effort)")
    ap.add_argument("--sample-out", default=None,
                    help="if given, write a deterministic 1%% sample of fact "
                         "(hash(s) %% 100 == 0, Spark hash) to this volume path "
                         "before running pipelines (D-SCALE 100M leg)")
    ap.add_argument("--distinct-out", default=None,
                    help="if given, compute exact per-key COUNT(DISTINCT s) "
                         "for kA (the local study's distinct_A, needed by "
                         "arms.run_arms) and write the 20,000-row result to "
                         "this volume path as parquet (D-SCALE 100M leg). "
                         "The execution phase merges it into counts.npz "
                         "before the local arms run; see README_JOB_DEFS.md. "
                         "distinct_A is used ONLY for the D_A pipeline's GT "
                         "load (D_A is not one of the 9 cloud-compared "
                         "pipelines), and D_A's verdict cannot flip on the "
                         "tiny (kA, s) pairing difference vs the local "
                         "generator (max distinct ~100,000 vs T_DISTINCT "
                         "50,000).")
    ap.add_argument("--labelling", default="Databricks serverless Jobs compute "
                    "-- topology proof via executor hostnames; see manifest")
    args = ap.parse_args()

    from pyspark.sql import SparkSession, functions as F, Window
    from pyspark import StorageLevel

    manifest_dir = os.path.dirname(args.manifest)
    eventlog_dir = args.eventlog_dir or os.path.join(manifest_dir, "eventlog")
    try:
        os.makedirs(eventlog_dir, exist_ok=True)
    except Exception as e:  # noqa: BLE001 - best-effort only
        print(f"eventlog dir not creatable ({e!r}); event-log path unavailable")
    spark = (SparkSession.builder.appName("BlindAtScale-GroundTruth-Databricks")
             .config("spark.sql.shuffle.partitions", str(P_PARTITIONS))
             .config("spark.sql.files.maxPartitionBytes", "8388608")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.databricks.photon.enabled", "false")
             .config("spark.eventLog.enabled", "true")
             .config("spark.eventLog.dir", "file://" + eventlog_dir)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    master = spark.sparkContext.master
    app_id = spark.sparkContext.applicationId
    photon_effective = spark.conf.get("spark.databricks.photon.enabled", "unset")
    d9_fired = str(photon_effective).lower() == "true"
    print(f"master={master} app_id={app_id} spark={spark.version} "
          f"photon_effective={photon_effective} D9_fired={d9_fired}")

    fact = spark.read.parquet(os.path.join(args.data, "fact.parquet"))
    fact2 = spark.read.parquet(os.path.join(args.data, "fact2.parquet"))
    dim = spark.read.parquet(os.path.join(args.data, "dim.parquet"))
    n_fact = fact.count()
    t_load = 0.02 * n_fact
    print(f"n_fact={n_fact} t_load={t_load}")

    sample_info = None
    if args.sample_out:
        sample = fact.filter((F.hash("s") % 100) == 0)
        n_sample = sample.count()
        sample.write.mode("overwrite").parquet(args.sample_out)
        sample_info = {"path": args.sample_out, "n_rows": int(n_sample),
                       "definition": "hash(s) % 100 == 0 (Spark murmur3 hash)"}
        print(f"1% sample: {n_sample} rows -> {args.sample_out}")

    distinct_info = None
    if args.distinct_out:
        # Exact per-key COUNT(DISTINCT s) for kA -- the local study's
        # distinct_A (code/gen_dataset.py). 20,000 rows: safe to collect.
        da_rows = (fact.groupBy("kA")
                   .agg(F.countDistinct("s").alias("d")).collect())
        da_max = max(int(r["d"]) for r in da_rows)
        spark.createDataFrame(da_rows, ["kA", "d"]).write.mode(
            "overwrite").parquet(args.distinct_out)
        distinct_info = {"path": args.distinct_out,
                         "n_keys": len(da_rows), "max_distinct": da_max,
                         "definition": ("COUNT(DISTINCT s) per kA -- the local "
                                        "study's distinct_A (D_A GT only)")}
        print(f"distinct_A export: {len(da_rows)} keys, max={da_max} "
              f"-> {args.distinct_out}")

    topo = executor_topology(spark)
    print(f"topology: {topo}")

    eventlog_state = {"dir": eventlog_dir, "available": False, "files": [],
                      "note": ("best-effort: on serverless the event log may "
                               "not be downloadable; D8 is verdict-bearing "
                               "regardless. Post-hoc parse uses the per-pipeline "
                               "[start_ms, end_ms] windows if the log is "
                               "retrieved later.")}
    try:
        eventlog_state["files"] = sorted(os.listdir(eventlog_dir))
        eventlog_state["available"] = len(eventlog_state["files"]) > 0
    except Exception as e:  # noqa: BLE001 - best-effort only
        eventlog_state["error"] = repr(e)[:300]

    manifest = {"spark_version": spark.version, "master": master,
                "app_id": app_id, "eventlog_dir": eventlog_dir,
                "n_fact": n_fact, "t_load": t_load,
                "shuffle_partitions": P_PARTITIONS,
                "labelling": args.labelling,
                "photon_effective": str(photon_effective),
                "deviation_D9_photon": ("FIRED: Photon on despite config; "
                                        "loads are Photon-executed" if d9_fired
                                        else "not fired (Photon off)"),
                "executor_topology": topo,
                "sample_export": sample_info,
                "distinct_export": distinct_info,
                "measurement": {
                    "verdict_bearing_source":
                        "partition_id (D8); F pipelines: analytic splitmix64 "
                        "from Spark-produced counts (D4)",
                    "why": ("On serverless Jobs compute the Spark event log "
                            "may not be downloadable. The D8 partition-id "
                            "measurement realises the same quantity the event "
                            "log's shuffle-read measures (raw rows in the "
                            "busiest post-shuffle partition) by an executed "
                            "shuffle, and is verdict-bearing. The event-log "
                            "path is kept as best-effort corroboration where "
                            "available."),
                    "event_log": eventlog_state},
                "pipelines": []}
    if os.path.exists(args.manifest):
        os.remove(args.manifest)

    def record(pid, start_ms, fn):
        entry = {"pipeline": pid, "start_ms": start_ms}
        try:
            info = fn()
            entry.update(info)
            entry["status"] = entry.get("status", "OK")
        except Exception as e:  # keep going: one failure must not lose the rest
            entry["status"] = "ERROR"
            entry["error"] = repr(e)[:500]
            print(f"[{pid}] ERROR {e!r}")
        entry["end_ms"] = int(time.time() * 1000)
        entry["wall_seconds"] = round((entry["end_ms"] - start_ms) / 1000.0, 2)
        # Verdict-bearing load + verdict (D8 primary; D4 for F).
        if "gt_load" not in entry:
            pc = entry.get("partition_id") or {}
            entry["gt_load"] = pc.get("max_partition_rows")
            entry["gt_load_source"] = "partition_id_D8"
        if entry.get("gt_load") is not None:
            entry["verdict"] = "FAIL" if entry["gt_load"] > t_load else "PASS"
            entry["t_load"] = t_load
        entry["event_log"] = {
            "available_in_job": False,
            "note": ("event-log task metrics are not available in-job; "
                     "post-hoc parse (if the log is downloadable) uses "
                     "this pipeline's [start_ms, end_ms] window."),
            "window": [entry["start_ms"], entry["end_ms"]]}
        manifest["pipelines"].append(entry)
        with open(args.manifest, "w") as fh:
            json.dump(manifest, fh, indent=1)
        print(f"[{pid}] {entry['status']} wall={entry['wall_seconds']}s "
              f"gt_load={entry.get('gt_load')} verdict={entry.get('verdict')} "
              f"info={ {k: v for k, v in entry.items() if k not in ('pipeline','start_ms','end_ms','partition_id')} }")
        spark.catalog.clearCache()

    def do_group(col):
        def run():
            rep = fact.repartition(P_PARTITIONS, col)          # D2 stage 1
            rep.persist(StorageLevel.MEMORY_AND_DISK)
            pc = partition_counts(rep, spark)                  # D8
            agg = rep.groupBy(col).agg(F.count("*").alias("c"))
            rows = agg.collect()
            rep.unpersist()
            return {"partition_id": pc,
                    "result_rows": len(rows),
                    "max_group_count": int(max(r["c"] for r in rows))}
        return run

    def do_join(col):
        def run():
            joined = fact.join(dim.hint("SHUFFLE_MERGE"),
                               fact[col] == dim["dim_key"], "inner")
            rep = joined.repartition(P_PARTITIONS, col)        # D8 on join output
            rep.persist(StorageLevel.MEMORY_AND_DISK)
            pc = partition_counts(rep, spark)
            n_out = rep.count()
            rep.unpersist()
            return {"partition_id": pc, "join_output_rows": int(n_out)}
        return run

    def do_factfact(col):
        def run():
            c1 = fact.groupBy(col).agg(F.count("*").alias("c1"))
            c2 = fact2.groupBy(col).agg(F.count("*").alias("c2"))
            j = c1.join(c2, col).withColumn("prod", F.col("c1") * F.col("c2"))
            rows = j.collect()  # 20,000 keys -- safe to bring to the driver
            keys = np.array([r[col] for r in rows], dtype=np.int64)
            prod = np.zeros(20_000, dtype=np.float64)
            prod[keys] = np.array([r["prod"] for r in rows], dtype=np.float64)
            total_out = float(prod.sum())
            analytic = max_partition_load(prod)
            info = {"distinct_shared_keys": len(rows),
                    "exact_join_output_rows": total_out,
                    "analytic_load_splitmix64": analytic,
                    "analytic_note": "driver computation from Spark-produced "
                                     "per-key counts (deviation D4)",
                    "gt_load": analytic,
                    "gt_load_source": ("analytic_splitmix64_from_spark_counts "
                                       "(F pipeline; D4)")}
            if total_out <= args.raw_join_cap:
                raw = fact.join(fact2.hint("SHUFFLE_MERGE"),
                                fact[col] == fact2[col], "inner")
                info["join_output_rows"] = int(raw.count())
                info["status"] = "EXECUTED_RAW"
            else:
                info["status"] = "SKIPPED_NOT_MATERIALISABLE"
            return info
        return run

    def do_window():
        def run():
            rep = fact.repartition(P_PARTITIONS, "kA")         # D8: same partitioner
            pc = partition_counts(rep, spark)                  # as the window shuffle
            win = Window.partitionBy("kA").orderBy("s")
            w = fact.withColumn("rn", F.row_number().over(win))
            mx = w.agg(F.max("rn")).collect()[0][0]
            return {"partition_id": pc,
                    "max_window_partition_rows": int(mx)}
        return run

    impls = {
        "G_A": do_group("kA"), "G_B": do_group("kB"), "G_N": do_group("kN"),
        "J_A": do_join("kA"), "J_B": do_join("kB"), "J_N": do_join("kN"),
        "F_A": do_factfact("kA"), "F_B": do_factfact("kB"),
        "W_A": do_window(),
    }
    for pid in [p.strip() for p in args.pipelines.split(",") if p.strip()]:
        if pid not in impls:
            print(f"unknown pipeline {pid}, skipped")
            continue
        print(f"=== {pid} ===")
        record(pid, int(time.time() * 1000), impls[pid])

    # Final event-log availability re-check (log may have appeared mid-run).
    try:
        eventlog_state["files"] = sorted(os.listdir(eventlog_dir))
        eventlog_state["available"] = len(eventlog_state["files"]) > 0
    except Exception as e:  # noqa: BLE001 - best-effort only
        eventlog_state["error"] = repr(e)[:300]
    with open(args.manifest, "w") as fh:
        json.dump(manifest, fh, indent=1)

    spark.stop()
    print(f"manifest -> {args.manifest}")


if __name__ == "__main__":
    main()
