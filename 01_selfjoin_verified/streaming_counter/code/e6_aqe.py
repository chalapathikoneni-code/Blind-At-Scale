"""STEP 3/4 - E6, AQE robustness. Run TWICE by the step script:
once with AQE off, once with AQE on (spark-submit --conf), same
inputs, same executor config. Only the AQE confs differ.

Cases (select with --cases):
  join_dim   fact JOIN dim_authors ON author_id
  join_meta  fact JOIN works_meta  ON work_id   (near-uniform control)
  selfjoin   fact JOIN fact        ON author_id (join-emergent skew)

For each executed case the job records: output rows, per-partition
loads (streamed, never glom'd), and the executed physical plan text
- the plan is where AQE skew-join handling (AQEShuffleRead /
OptimizedSkewedJoin) shows up if it engaged.

Safety guard (recovery patch): a case whose synopsis prediction
exceeds MAX_OUTPUT_ROWS or MAX_LOAD_ROWS is recorded SKIPPED_GUARD
with its exact predicted values and never materialized (this is why
the author_id self-join is only ever measured on the 1-part tables).
"""
import argparse
import statistics
import time

from pyspark.sql import SparkSession

from common import (PARTITIONS, MAX_OUTPUT_ROWS, MAX_LOAD_ROWS,
                    predict_from_counts, verdict, write_json,
                    count_per_partition)


def counts_dict(df, key):
    return {r[0]: r[1] for r in df.groupBy(key).count().collect()}


def plan_text(df):
    return df._jdf.queryExecution().executedPlan().toString()


def run_case(spark, name, left, right, on, tag, out_prefix,
             self_join=False):
    t0 = time.time()
    left_counts = counts_dict(left, on)
    right_counts = left_counts if self_join else counts_dict(right, on)
    pred = predict_from_counts(left_counts,
                               None if self_join else right_counts)
    rec = {
        "case": name, "aqe": tag,
        "aqe_enabled_conf": spark.conf.get("spark.sql.adaptive.enabled"),
        "self_join": self_join,
        "predicted_output_rows": pred["total_rows"],
        "predicted_max_load": pred["max_load"],
    }
    if self_join:
        rec["predicted_sum_count_squared"] = pred["total_rows"]

    if (pred["total_rows"] > MAX_OUTPUT_ROWS
            or pred["max_load"] > MAX_LOAD_ROWS):
        rec["status"] = "SKIPPED_GUARD"
        rec["reason"] = (
            "predicted output %d (cap %d) / predicted max load %d "
            "(cap %d)" % (pred["total_rows"], MAX_OUTPUT_ROWS,
                          pred["max_load"], MAX_LOAD_ROWS))
        rec["elapsed_sec"] = round(time.time() - t0, 1)
        write_json(spark, rec,
                   "%s/e6_%s_%s.json" % (out_prefix, tag, name))
        print(name, tag, "SKIPPED_GUARD pred_max_load",
              pred["max_load"])
        return

    joined = left.join(right, on=on, how="inner")
    plan = plan_text(joined)
    joined = joined.repartition(PARTITIONS, on)
    per_part = count_per_partition(joined.rdd)
    total = sum(per_part)
    rec.update({
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
    })
    write_json(spark, rec, "%s/e6_%s_%s.json" % (out_prefix, tag, name))
    print(name, tag, "max load", rec["max_partition_load"],
          "skew-join in plan:", rec["plan_has_skew_join_opt"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--s3-prefix", required=True)
    ap.add_argument("--aqe", required=True, choices=["on", "off"])
    ap.add_argument("--cases", default="all",
                    help="comma list of join_dim,join_meta,selfjoin "
                         "or 'all'")
    args = ap.parse_args()
    prefix = args.s3_prefix.rstrip("/")
    sel = (None if args.cases == "all"
           else {c.strip() for c in args.cases.split(",")})

    def want(c):
        return sel is None or c in sel

    spark = SparkSession.builder.appName(
        "bas-e1e6-e6-" + args.aqe).getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    fact = spark.read.parquet(prefix + "/data/fact_authorships")
    if want("join_dim"):
        dim = spark.read.parquet(prefix + "/data/dim_authors")
        run_case(spark, "join_dim", fact, dim.select("author_id"),
                 "author_id", args.aqe, prefix + "/output")
    if want("join_meta"):
        meta = spark.read.parquet(prefix + "/data/works_meta")
        run_case(spark, "join_meta", fact, meta.select("work_id"),
                 "work_id", args.aqe, prefix + "/output")
    if want("selfjoin"):
        run_case(spark, "selfjoin", fact, fact, "author_id",
                 args.aqe, prefix + "/output", self_join=True)
    spark.stop()


if __name__ == "__main__":
    main()
