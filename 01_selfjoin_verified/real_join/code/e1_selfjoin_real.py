"""REAL (materialized) OpenAlex self-join - Blind at Scale.

Answers the reviewer objection that the Nov 25 runs used a streaming
per-partition counter (count_per_partition under --force-measure) and
therefore "measured the load distribution without executing the join."

This job executes the ACTUAL self-join and MATERIALIZES it: the joined
DataFrame is written to S3 Parquet. No count_per_partition, no glom, no
count-based shortcut is used for the join itself. Default sort-merge
join (autoBroadcastJoinThreshold = -1, no broadcast hint).

Frozen contract (do not change thresholds, partitioning, or prediction
math):
  - predict_from_counts(exact counts, fanout=None) gives the frozen
    prediction; it is written to result_pre.json BEFORE the join runs.
  - the guard (caps MAX_OUTPUT_ROWS / MAX_LOAD_ROWS) trips on the
    predicted hottest partition; --force-measure overrides it exactly
    as Nov 25 did (guard_overridden: true + would-trip reason). The
    override changes no threshold and no prediction; it only lets the
    job run.
  - AQE stays OFF and spark.sql.shuffle.partitions stays 128 (set at
    submit time) so the static 128-partition hash distribution the
    prediction assumes is the one actually executed.
"""
import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import (PARTITIONS, MAX_OUTPUT_ROWS, MAX_LOAD_ROWS,
                    predict_from_counts, verdict, write_json)


def counts_dict(df, key):
    return {r[0]: r[1] for r in df.groupBy(key).count().collect()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--s3-prefix", required=True,
                    help="data location, reads <prefix>/data/fact_authorships")
    ap.add_argument("--out-prefix", required=True,
                    help="RealJoin output prefix for this scale")
    ap.add_argument("--scale", required=True)
    ap.add_argument("--force-measure", action="store_true")
    args = ap.parse_args()
    data = args.s3_prefix.rstrip("/")
    outp = args.out_prefix.rstrip("/")

    spark = SparkSession.builder.appName(
        "bas-realjoin-" + args.scale).getOrCreate()
    # Default sort-merge join; no broadcast.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    fact = spark.read.parquet(data + "/data/fact_authorships")
    left_rows = fact.count()

    # Frozen prediction - exact per-key counts, self-join fan-out.
    counts = counts_dict(fact, "author_id")
    pred = predict_from_counts(counts, None)
    est_output = pred["total_rows"]
    est_max = pred["max_load"]
    pre = {
        "case": "selfjoin_real", "scale": args.scale,
        "join_key": "author_id", "self_join": True,
        "materialized": True, "left_rows": left_rows,
        "predicted_output_rows": est_output,
        "predicted_max_load": est_max,
        "predicted_sum_count_squared": est_output,
        "predicted_verdict": verdict(est_max, est_output),
        "frozen_config": {
            "aqe": "off", "shuffle_partitions": PARTITIONS,
            "num_executors": 18, "executor_memory": "8g",
            "executor_cores": 4, "driver_memory": "8g",
            "join": "sort-merge, autoBroadcastJoinThreshold=-1"},
    }
    guard_tripped = (est_output > MAX_OUTPUT_ROWS
                     or est_max > MAX_LOAD_ROWS)
    if guard_tripped:
        pre["guard_overridden"] = bool(args.force_measure)
        pre["guard_would_trip_reason"] = (
            "predicted output %d (cap %d) / predicted max load %d "
            "(cap %d)" % (est_output, MAX_OUTPUT_ROWS,
                          est_max, MAX_LOAD_ROWS))

    # Predictions on record BEFORE the join launches.
    write_json(spark, pre, outp + "/result_pre.json")

    if guard_tripped and not args.force_measure:
        pre["status"] = "SKIPPED_GUARD"
        write_json(spark, pre, outp + "/result.json")
        spark.stop()
        return

    # REAL materialized self-join. Alias both sides to resolve the
    # self-join column ambiguity; write the joined rows to Parquet.
    left = fact.alias("l")
    right = fact.alias("r")
    joined = (left.join(right,
                        F.col("l.author_id") == F.col("r.author_id"),
                        "inner")
              .select(F.col("l.work_id").alias("work_id_left"),
                      F.col("r.work_id").alias("work_id_right"),
                      F.col("l.author_id").alias("author_id")))
    out_data = outp + "/joined.parquet"
    t0 = time.time()
    joined.write.mode("overwrite").parquet(out_data)
    elapsed = round(time.time() - t0, 1)

    # Authoritative output row count from the written dataset
    # (parquet footer counts - not a shortcut for the join, which
    # already executed and materialized above).
    out_rows = spark.read.parquet(out_data).count()

    result = dict(pre)
    result["status"] = "OK"
    result["output_parquet"] = out_data
    result["measured_output_rows"] = out_rows
    result["elapsed_sec"] = elapsed
    write_json(spark, result, outp + "/result.json")
    print("REALJOIN", args.scale, "materialized rows", out_rows,
          "elapsed", elapsed)
    spark.stop()


if __name__ == "__main__":
    main()
