"""Spark implementations of the Blind at Scale headline pipelines, executed
for REAL on a Spark standalone cluster (see setup.sh), with ground-truth
loads taken from the Spark EVENT LOG -- not from a simulation.

Pipelines (PREREGISTRATION.md S.2; this workstream's scope):
  G_A, G_B, G_N   GROUP BY on fact key kA / kB / kN
  J_A, J_B, J_N   fact JOIN dim on kA / kB / kN
  F_A, F_B        fact JOIN fact2 on kA / kB   (guarded -- see D4 below)
  W_A             window partitioned by kA
All run with spark.sql.shuffle.partitions = 128 (matching P = 128 locally).

=========================================================================
GROUND-TRUTH LOAD DEFINITION (Spark) -- read before citing any number
=========================================================================
Pipelines run strictly sequentially; run_manifest.json records the wall
clock window [start_ms, end_ms] of each. For pipeline P, Tasks(P) = every
task whose TaskEnd event falls in P's window. For a task t, from its
TaskMetrics in the event log:

  input_records(t)        = Input Metrics.Records Read          (else 0)
  shuffle_read_records(t) = Shuffle Read Metrics.Total Records Read (else 0)
  shuffle_write_records(t)= Shuffle Write Metrics.Records Written   (else 0)

  GT_LOAD(P) = max over t in Tasks(P) of shuffle_read_records(t)
  VERDICT(P) = FAIL iff GT_LOAD(P) > T_LOAD, T_LOAD = 200,000 at the
               headline scale (T = 0.02 * N_fact, PREREGISTRATION.md S.3).

  Why shuffle-read, not input+shuffle-read: the local loads are
  per-partition (post-shuffle) quantities -- the records a single task
  must hold/process for one hash partition of the data. A task's
  shuffle-read records are exactly that in Spark. Its INPUT records, by
  contrast, are governed by parquet file splitting (a storage artefact:
  with the single-file datasets gen_on_pod.py writes, one scan task can
  read millions of rows that it immediately shuffles out in small
  pieces -- in the local smoke test the scan task read all 500,000 rows
  while the busiest shuffle partition held ~50k). Input records are
  therefore captured and reported (max_task_input_records,
  gt_load_incl_input = max_t (input+shuffle_read)) but are NOT the
  verdict-bearing load. (This refines the workstream brief's
  "input+shuffle" phrasing; both quantities are in the output JSON so
  either can be recomputed.)

Also captured per pipeline (collect_results.py does the extraction):
  max_task_shuffle_bytes  = max_t (shuffle read bytes + shuffle write bytes)
  max_task_input_records  = max_t input_records(t)
  max_task_load_incl_write= max_t (load_records(t)+shuffle_write_records(t))
  spill_bytes_max_task / spill_bytes_total = Memory+Disk Bytes Spilled
  max_task_time_ms        = max_t Executor Run Time
  stage_durations         = per-stage Completion-Submission times

=========================================================================
DEVIATIONS FROM THE LOCAL SIMULATION (PREREGISTRATION.md S.2) -- all of
these are real properties of executing in Spark; none is hidden.
=========================================================================
D1 HASH FUNCTION. Local loads hash keys with splitmix64; Spark's
   HashPartitioning uses murmur3 (seed 42). Partition assignment differs;
   max-partition magnitudes are comparable, never identical.
D2 GROUP BY (G_*). Local load = max GROUP count. Spark aggregates with
   map-side partial aggregation, so the aggregation shuffle itself
   carries per-(key, input-partition) partial counts, not raw rows. To
   expose the raw-row partition load, each G pipeline FIRST performs an
   explicit repartition(128, key); GT_LOAD is then realised by that
   shuffle's read tasks = raw fact rows in the busiest murmur3 partition
   = sum of the group counts of all keys colliding into that partition.
   This UPPER-BOUNDS the local max-group load. Both stages are in the
   event log; collect_results.py reports the per-stage breakdown so the
   partial-aggregation stage can be inspected separately.
D3 FACT-DIM JOIN (J_*). Local load = max splitmix64-partition load of
   join OUTPUT rows. Here broadcast joins are DISABLED
   (autoBroadcastJoinThreshold=-1) and a SHUFFLE_MERGE join is forced, so
   both inputs are shuffled on the join key with murmur3. Because dim
   contains every key exactly once, join output rows per partition =
   fact rows per partition (except J_N: fact rows with kN = -1 sentinel
   match no dim key and drop out -- mirroring the local J_N masking of
   the NULL group). GT_LOAD is realised by the fact-side shuffle-read
   tasks and equals max-partition fact rows, i.e. the local definition
   modulo D1.
D4 FACT-FACT JOIN (F_*). The RAW join output is not materialisable --
   that is the failure phenomenon itself (Amendment 1b): for zipf/101
   F_A, sum_k c1(k)*c2(k) is ~5.5e11 rows and the single hottest key
   alone contributes 5.47e11. Running it would consume the pod budget
   without finishing, so this script does NOT run the raw join when the
   exact output size (computed from Spark-executed per-key counts)
   exceeds --raw-join-cap rows (default 2e8). Instead it executes, in
   Spark, the aggregate form -- groupBy counts of both facts (real
   shuffles), join of the two 20,000-row count tables -- and reports:
     spark_executed_*     : task metrics of those real Spark stages;
     analytic_load_splitmix64 : max-partition load of c1(k)*c2(k) under
        the LOCAL splitmix64 definition, computed in the driver from
        the Spark-produced counts (identical to the local F ground
        truth if the counts match -- a cross-check, NOT a Spark task
        metric; labelled as such in every output);
     raw_join_status      : "SKIPPED_NOT_MATERIALISABLE" + exact rows.
   If the output ever fits under the cap, the raw join IS run
   (status "EXECUTED_RAW") -- but note its shuffle-read task load still
   measures the join INPUT partitions, not the output: the joined rows
   stream into the counting aggregate without a further shuffle. For
   F pipelines the verdict-bearing load is therefore ALWAYS the
   analytic splitmix64 load from Spark-produced counts, with the task
   metrics reported alongside; the raw run's value is its spill and
   task-time evidence (and its output count, which in the smoke test
   matched the analytic total exactly: 44,243,713 rows).
D5 WINDOW (W_A). Local load = max group count of kA (declared proxy).
   Spark's window shuffles fact by kA and sorts within partitions;
   GT_LOAD is realised by the shuffle-read tasks (raw rows in the
   busiest partition), the same upper-bound relationship as D2.
D6 SCAN SPLITS. input_records(t) for parquet scan tasks depends on
   Spark's file splitting; this script sets
   spark.sql.files.maxPartitionBytes = 8 MB to bound scan-task size,
   and gen_on_pod.py writes single files. Input records are reported
   but never verdict-bearing (see the definition above) -- in the local
   smoke run the scan task's 500,000 input records would otherwise
   have swamped every pipeline's true partition load (~50k).
D7 AQE OFF. spark.sql.adaptive.enabled=false: Spark 3.5 AQE would
   coalesce shuffle partitions and split skewed partitions at runtime,
   changing the realised loads. Disabling it keeps the execution the
   plain 128-partition plan the local simulation models. This is a
   modelling choice, stated here, not an oversight.

Usage (on the pod, after setup.sh + gen_on_pod.py):
  spark-submit --master spark://127.0.0.1:7077 \
      --driver-memory 2g --total-executor-cores <cores> \
      spark_pipelines.py --data ~/bas-data/zipf_s101 \
      --manifest ~/bas-spark/run_manifest.json
  (--master local[N] also works for a smoke run; the manifest records
   the master actually used, and collect_results.py copies it into the
   output so a local run can never be mistaken for cluster ground truth.)
"""
import argparse, json, os, time
import numpy as np

P_PARTITIONS = 128
T_LOAD_HEADLINE = 200_000

# splitmix64, copied from code/common.py (local study) -- used ONLY for the
# clearly-labelled analytic F cross-check (deviation D4).
def splitmix64_hash(keys):
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="dir with fact/fact2/dim parquet")
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--pipelines", default="G_A,G_B,G_N,J_A,J_B,J_N,F_A,F_B,W_A")
    ap.add_argument("--raw-join-cap", type=float, default=2e8)
    ap.add_argument("--eventlog-dir", default=None,
                    help="default: $EVENTLOG_DIR or ~/bas-spark/eventlog")
    args = ap.parse_args()

    from pyspark.sql import SparkSession, functions as F, Window

    eventlog_dir = (args.eventlog_dir or os.environ.get("EVENTLOG_DIR")
                    or os.path.expanduser("~/bas-spark/eventlog"))
    os.makedirs(eventlog_dir, exist_ok=True)
    spark = (SparkSession.builder.appName("BlindAtScale-GroundTruth")
             .config("spark.sql.shuffle.partitions", str(P_PARTITIONS))
             .config("spark.sql.files.maxPartitionBytes", "8388608")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.eventLog.enabled", "true")
             .config("spark.eventLog.dir", "file://" + eventlog_dir)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    master = spark.sparkContext.master
    app_id = spark.sparkContext.applicationId
    print(f"master={master} app_id={app_id} spark={spark.version}")

    fact = spark.read.parquet(os.path.join(args.data, "fact.parquet"))
    fact2 = spark.read.parquet(os.path.join(args.data, "fact2.parquet"))
    dim = spark.read.parquet(os.path.join(args.data, "dim.parquet"))
    n_fact = fact.count()  # also warms the cache of file metadata
    print(f"n_fact={n_fact}")

    manifest = {"spark_version": spark.version, "master": master,
                "app_id": app_id, "eventlog_dir": eventlog_dir,
                "n_fact": n_fact, "shuffle_partitions": P_PARTITIONS,
                "labelling": ("multi-worker standalone cluster on a single host"
                              if master.startswith("spark://") else
                              f"NOT A CLUSTER RUN -- master={master}"),
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
        manifest["pipelines"].append(entry)
        with open(args.manifest, "w") as fh:
            json.dump(manifest, fh, indent=1)
        print(f"[{pid}] {entry['status']} wall={entry['wall_seconds']}s "
              f"info={ {k: v for k, v in entry.items() if k not in ('pipeline','start_ms','end_ms')} }")
        spark.catalog.clearCache()

    def do_group(col):
        def run():
            rep = fact.repartition(P_PARTITIONS, col)          # D2 stage 1
            agg = rep.groupBy(col).agg(F.count("*").alias("c"))
            rows = agg.collect()
            return {"result_rows": len(rows),
                    "max_group_count": int(max(r["c"] for r in rows))}
        return run

    def do_join(col):
        def run():
            joined = fact.join(dim.hint("SHUFFLE_MERGE"),
                               fact[col] == dim["dim_key"], "inner")
            return {"join_output_rows": int(joined.count())}
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
            info = {"distinct_shared_keys": len(rows),
                    "exact_join_output_rows": total_out,
                    "analytic_load_splitmix64": max_partition_load(prod),
                    "analytic_note": "driver computation from Spark-produced "
                                     "per-key counts (deviation D4)"}
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
            win = Window.partitionBy("kA").orderBy("s")
            w = fact.withColumn("rn", F.row_number().over(win))
            mx = w.agg(F.max("rn")).collect()[0][0]
            return {"max_window_partition_rows": int(mx)}
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

    spark.stop()
    print(f"manifest -> {args.manifest}")


if __name__ == "__main__":
    main()
