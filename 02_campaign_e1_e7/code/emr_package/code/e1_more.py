"""STEP 6 - E1 at more scales + a second natural join family.

Scales (more REAL data each time - rows are never duplicated):
  s4   the core PREP extract (4 works parts, ~1.54M works)
  s8   8 parts  (~3.1M works)
  s12  12 parts (~4.6M works, ~12.6 GB staged raw parquet)
The authorship cases (join_dim, selfjoin) run at s8 and s12 here;
s4 numbers come from core Step 2.

Second family (from the s4 extract, no extra download):
works' primary venue - fact_work_source (work_id, source_id) joined
to dim_sources (source_id, display_name), plus the source self-join
(venue fan-out: a venue's works x themselves).
Cases run through the same run_case as core E1 (imported).
"""
import argparse
import json
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from e1_join import run_case
from prep_openalex import stage
from common import write_json

MANIFEST = ("https://openalex.s3.amazonaws.com/data/parquet/works/"
            "manifest.json")


def pick_n(n):
    with urllib.request.urlopen(MANIFEST, timeout=120) as fh:
        manifest = json.load(fh)
    files = sorted(manifest["files"],
                   key=lambda f: -f["meta"]["content_length"])
    return files[:n]


def build_fact(spark, dests, out_path):
    works = spark.read.parquet(*dests)
    exploded = works.select(
        F.col("id").alias("work_id"),
        F.explode("authorships").alias("a"))
    fact = exploded.select(
        "work_id",
        F.col("a.author.id").alias("author_id"),
    ).filter(F.col("author_id").isNotNull())
    fact.write.mode("overwrite").parquet(out_path)
    return fact


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--s3-prefix", required=True)
    args = ap.parse_args()
    prefix = args.s3_prefix.rstrip("/")

    spark = SparkSession.builder.appName("bas-e1e6-e1-more").getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    files12 = pick_n(12)
    dests = stage(files12, prefix + "/raw_works")  # parts 0..11
    for n, tag in ((8, "s8"), (12, "s12")):
        fact = build_fact(spark, dests[:n],
                          "%s/data/fact_authorships_%s" % (prefix, tag))
        dim = (fact.select("author_id").distinct())
        run_case(spark, "join_dim_" + tag, fact, dim, "author_id",
                 False, prefix + "/output")
        run_case(spark, "selfjoin_" + tag, fact, fact, "author_id",
                 True, prefix + "/output")

    # Second family: works x sources (venues), from the s4 parts.
    works = spark.read.parquet(*dests[:4])
    ws = works.select(
        F.col("id").alias("work_id"),
        F.col("primary_location.source.id").alias("source_id"),
        F.col("primary_location.source.display_name")
        .alias("display_name"),
    ).filter(F.col("source_id").isNotNull())
    ws.write.mode("overwrite").parquet(prefix + "/data/fact_work_source")
    dim_sources = ws.select("source_id", "display_name").distinct()
    dim_sources.write.mode("overwrite").parquet(
        prefix + "/data/dim_sources")
    print("WORK-SOURCE rows:", ws.count(),
          "| sources:", dim_sources.count())
    run_case(spark, "join_source_dim", ws.select("work_id", "source_id"),
             dim_sources.select("source_id"), "source_id", False,
             prefix + "/output")
    run_case(spark, "selfjoin_source", ws.select("work_id", "source_id"),
             ws.select("work_id", "source_id"), "source_id", True,
             prefix + "/output")
    write_json(spark, {"step": "e1_more", "status": "OK",
                       "scales": ["s8", "s12"],
                       "family2": "works x sources"},
               prefix + "/output/e1_more_summary.json")
    spark.stop()


if __name__ == "__main__":
    main()
