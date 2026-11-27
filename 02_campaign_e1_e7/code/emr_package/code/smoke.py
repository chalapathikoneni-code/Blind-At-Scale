"""STEP 0 - SMOKE. Runs in ~2 minutes.

Proves the whole path end to end before anything expensive:
S3 write, spark-submit args, a real join, event logging.
Uses only data generated in the job. No downloads, no installs.
"""
import argparse
import random
import time

from pyspark.sql import SparkSession

from common import write_json


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="s3://.../output prefix")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("bas-e1e6-smoke").getOrCreate()
    t0 = time.time()
    rng = random.Random(42)
    # Tiny skewed join: 20,000 fact rows over 200 keys, Zipf-ish.
    keys = [int(200 * (rng.random() ** 3)) for _ in range(20000)]
    fact = spark.createDataFrame(
        [(i, k) for i, k in enumerate(keys)], ["id", "k"])
    dim = spark.createDataFrame([(k, "v%d" % k) for k in range(200)],
                                ["k", "label"])
    joined = fact.join(dim, on="k", how="inner")
    n = joined.count()
    per_part = joined.rdd.glom().map(len).collect()
    result = {
        "step": "smoke",
        "joined_rows": n,
        "max_partition_rows": max(per_part),
        "num_partitions": len(per_part),
        "executors_seen": len(
            spark.sparkContext._jsc.sc().statusTracker()
            .getExecutorInfos()),
        "elapsed_sec": round(time.time() - t0, 1),
        "status": "OK",
    }
    print(result)
    write_json(spark, result, args.out + "/smoke.json")
    spark.stop()


if __name__ == "__main__":
    main()
