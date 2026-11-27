"""STEP 7 - E6 on the synthetic skew cases (J_A / J_N style).

The known A1-miss cases from the 100M synthetic study: fact JOIN
dim on kA (J_A) and on kN (J_N). Run twice by the step script, AQE
off then on, identical inputs. Inputs are the Nov 23 synthetic
parquet in S3; if the dim parquet's key column differs, the job
falls back to a dim built from the fact's distinct keys (printed
in the log either way).
"""
import argparse

from pyspark.sql import SparkSession

from e6_aqe import run_case


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--s3-prefix", required=True)
    ap.add_argument("--synthetic-prefix",
                    default="s3://YOUR_BUCKET/Access/synthetic")
    ap.add_argument("--aqe", required=True, choices=["on", "off"])
    args = ap.parse_args()
    prefix = args.s3_prefix.rstrip("/")
    syn = args.synthetic_prefix.rstrip("/")

    spark = SparkSession.builder.appName(
        "bas-e1e6-e6synth-" + args.aqe).getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    fact = spark.read.parquet(syn + "/data/fact.parquet")
    print("FACT SCHEMA:"); fact.printSchema()
    try:
        dim = spark.read.parquet(syn + "/data/dim.parquet")
        print("DIM SCHEMA:"); dim.printSchema()
    except Exception as exc:  # dim absent - build from fact keys
        print("dim.parquet unreadable (%s); building from fact" % exc)
        dim = None
    for case, col in (("synth_J_A", "kA"), ("synth_J_N", "kN")):
        if dim is not None and col in dim.columns:
            right = dim.select(col).distinct()
        else:
            right = fact.select(col).distinct()
        run_case(spark, case, fact, right, col, args.aqe,
                 prefix + "/output")
    spark.stop()


if __name__ == "__main__":
    main()
