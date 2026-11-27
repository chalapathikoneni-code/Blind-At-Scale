"""STEP 2 - E1, real join validation on the OpenAlex tables.

Cases (all on author_id / work_id keys from the PREP tables):
  join_dim    fact JOIN dim_authors  ON author_id  (fan-out 1)
  selfjoin    fact JOIN fact         ON author_id  (fan-out = the
              author's own work count - the join-emergent case)
  join_meta   fact JOIN works_meta   ON work_id    (fan-out 1,
              near-uniform key - the expected-PASS control)

For every case, three prediction arms run FIRST from reduced data,
then the FULL join executes anyway, so prediction and measurement
are paired:
  synopsis    exact per-key counts of both sides (count packet)
  endbiased   exact counts for the top-64 keys + a 1% sample of the
              tail, scaled up (heavy-hitter packet)
  uniform     a 1% row sample of both sides, scaled up

Predictions use crc32 key-to-partition assignment (documented
approximation); the event log holds the authoritative physical
measurement. Safety: a case whose predicted output exceeds
MAX_OUTPUT_ROWS is skipped with the reason recorded, never run.
"""
import argparse
import statistics
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import PARTITIONS, predict_from_counts, verdict, write_json

MAX_OUTPUT_ROWS = 2_000_000_000
TOPK = 64
SAMPLE_FRAC = 0.01
SEED = 42


def counts_dict(df, key):
    return {r[0]: r[1] for r in
            df.groupBy(key).count().collect()}


def sampled_counts(df, key, frac):
    n = df.count()
    s = df.sample(False, frac, SEED).groupBy(key).count().collect()
    return {r[0]: r[1] / frac for r in s}, n


def arm_predictions(left_counts, right_counts, self_join,
                    left_sampled, right_sampled):
    """Return {arm: predicted max partition load + total}.

    synopsis   exact per-key counts (the full count packet)
    endbiased  exact counts for the top-64 keys; the tail comes from
               the scaled 1% sample (heavy-hitter packet)
    uniform    scaled 1% sample counts on both sides
    """
    out = {}
    fan = None if self_join else right_counts
    out["synopsis"] = predict_from_counts(left_counts, fan)
    top = dict(sorted(left_counts.items(), key=lambda kv: -kv[1])[:TOPK])
    eb = dict(top)
    for k, v in left_sampled.items():
        if k not in eb:
            eb[k] = v
    if self_join:
        out["endbiased"] = predict_from_counts(eb, None)
        out["uniform"] = predict_from_counts(left_sampled, None)
    else:
        eb_fan = dict(right_counts)
        out["endbiased"] = predict_from_counts(eb, eb_fan)
        out["uniform"] = predict_from_counts(left_sampled,
                                             right_sampled)
    return out


def run_case(spark, name, left, right, on, self_join, out_prefix):
    t0 = time.time()
    left_counts = counts_dict(left, on)
    right_counts = left_counts if self_join else counts_dict(right, on)
    lc_s, _ = sampled_counts(left, on, SAMPLE_FRAC)
    rc_s = lc_s if self_join else sampled_counts(right, on,
                                                 SAMPLE_FRAC)[0]
    preds = arm_predictions(left_counts, right_counts, self_join,
                            lc_s, rc_s)

    est_output = preds["synopsis"]["total_rows"]
    rec = {"case": name, "join_key": on, "self_join": self_join,
           "left_rows": left.count(), "arms": {}}
    for arm, p in preds.items():
        v = verdict(p["max_load"], p["total_rows"])
        rec["arms"][arm] = {"predicted_max_load": p["max_load"],
                            "predicted_output_rows": p["total_rows"],
                            **v}
    if est_output > MAX_OUTPUT_ROWS:
        rec["status"] = "SKIPPED_GUARD"
        rec["reason"] = ("predicted output %d > cap %d"
                         % (est_output, MAX_OUTPUT_ROWS))
        write_json(spark, rec, "%s/e1_%s.json" % (out_prefix, name))
        print(rec)
        return

    joined = left.join(right, on=on, how="inner")
    joined = joined.repartition(PARTITIONS, on)
    per_part = joined.rdd.glom().map(len).collect()
    total = sum(per_part)
    measured = {"output_rows": total,
                "max_partition_load": max(per_part),
                "median_partition_load": statistics.median(per_part),
                "partition_loads": per_part,
                **verdict(max(per_part), total)}
    rec["measured"] = measured
    rec["status"] = "OK"
    rec["elapsed_sec"] = round(time.time() - t0, 1)
    write_json(spark, rec, "%s/e1_%s.json" % (out_prefix, name))
    print(name, "measured max load", measured["max_partition_load"],
          "of", total, "rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--s3-prefix", required=True)
    args = ap.parse_args()
    prefix = args.s3_prefix.rstrip("/")
    spark = SparkSession.builder.appName("bas-e1e6-e1").getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    fact = spark.read.parquet(prefix + "/data/fact_authorships")
    dim = spark.read.parquet(prefix + "/data/dim_authors")
    meta = spark.read.parquet(prefix + "/data/works_meta")

    run_case(spark, "join_dim", fact, dim.select("author_id"),
             "author_id", False, prefix + "/output")
    run_case(spark, "selfjoin", fact, fact, "author_id", True,
             prefix + "/output")
    run_case(spark, "join_meta", fact, meta.select("work_id"),
             "work_id", False, prefix + "/output")
    spark.stop()


if __name__ == "__main__":
    main()
