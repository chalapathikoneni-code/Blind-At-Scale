"""STEP 3 - E6, AQE robustness. Run TWICE by the step script:
once with AQE off, once with AQE on (spark-submit --conf), same
inputs, same executor config. Only the AQE confs differ.

For each case the job records: output rows, per-partition loads of
the materialized join, and the executed physical plan text - the
plan is where AQE skew-join handling (AQEShuffleRead /
OptimizedSkewedJoin) shows up if it engaged.
"""
import argparse
import statistics
import time

from pyspark.sql import SparkSession

from common import PARTITIONS, verdict, write_json


def plan_text(df):
    return df._jdf.queryExecution().executedPlan().toString()


def run_case(spark, name, left, right, on, tag, out_prefix):
    t0 = time.time()
    joined = left.join(right, on=on, how="inner")
    plan = plan_text(joined)
    joined = joined.repartition(PARTITIONS, on)
    per_part = joined.rdd.glom().map(len).collect()
    total = sum(per_part)
    rec = {
        "case": name, "aqe": tag,
        "aqe_enabled_conf": spark.conf.get("spark.sql.adaptive.enabled"),
        "output_rows": total,
        "max_partition_load": max(per_part),
        "median_partition_load": statistics.median(per_part),
        "partition_loads": per_part,
        **verdict(max(per_part), total),
        "plan_has_aqe_shuffle_read": "AQEShuffleRead" in plan,
        "plan_has_skew_join_opt": ("Skew" in plan and "Join" in plan),
        "executed_plan": plan,
        "elapsed_sec": round(time.time() - t0, 1),
        "status": "OK",
    }
    write_json(spark, rec, "%s/e6_%s_%s.json" % (out_prefix, tag, name))
    print(name, tag, "max load", rec["max_partition_load"],
          "skew-join in plan:", rec["plan_has_skew_join_opt"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--s3-prefix", required=True)
    ap.add_argument("--aqe", required=True, choices=["on", "off"])
    args = ap.parse_args()
    prefix = args.s3_prefix.rstrip("/")
    spark = SparkSession.builder.appName(
        "bas-e1e6-e6-" + args.aqe).getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    fact = spark.read.parquet(prefix + "/data/fact_authorships")
    dim = spark.read.parquet(prefix + "/data/dim_authors")
    run_case(spark, "join_dim", fact, dim.select("author_id"),
             "author_id", args.aqe, prefix + "/output")
    run_case(spark, "selfjoin", fact, fact, "author_id",
             args.aqe, prefix + "/output")
    spark.stop()


if __name__ == "__main__":
    main()
