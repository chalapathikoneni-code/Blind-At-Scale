"""STEP 1 - PREP. Builds the two OpenAlex tables on the cluster.

Downloads a few OpenAlex works parquet part-files (public snapshot,
s3://openalex, anonymous read) straight into the job's S3 prefix with
the AWS CLI, then builds:

  fact_authorships.parquet  (work_id, author_id)   - one row per
                              (work, author) pair, exploded from the
                              works.authorships array
  dim_authors.parquet       (author_id, display_name) - distinct
                              authors, taken from the same authorships
  works_meta.parquet        (work_id, publication_year)

The join key is author_id. Prolific authors are the natural skew:
a small number of authors appear on very many works, most on one.
Prints row counts and the top-30 author frequencies so the skew is
visible in the step log. Standard library + pyspark only.
"""
import argparse
import json
import subprocess
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MANIFEST = ("https://openalex.s3.amazonaws.com/data/parquet/works/"
            "manifest.json")
N_PARTS = 4            # largest works parts: ~1.05 GB each, ~400k works
                       # (verified live 2026-11-25: 4 parts = 4.23 GB,
                       # 1,539,526 works - under the 6 GB cap)
MAX_STAGE_BYTES = 6 * 1024 ** 3


def pick_parts():
    """Fetch the OpenAlex works manifest; return the N largest parts."""
    with urllib.request.urlopen(MANIFEST, timeout=120) as fh:
        manifest = json.load(fh)
    files = sorted(manifest["files"],
                   key=lambda f: -f["meta"]["content_length"])
    chosen = files[:N_PARTS]
    total = sum(f["meta"]["content_length"] for f in chosen)
    assert total <= MAX_STAGE_BYTES, "staged bytes over cap"
    return manifest.get("date"), chosen, total


def stage(chosen, raw_prefix):
    """Copy each part from the public bucket into our prefix."""
    dests = []
    for i, f in enumerate(chosen):
        dest = "%s/part_%04d.parquet" % (raw_prefix, i)
        subprocess.run(
            ["aws", "s3", "cp", "--no-sign-request", f["url"], dest],
            check=True)
        dests.append(dest)
        print("STAGED", f["url"], "->", dest,
              f["meta"]["content_length"], "bytes,",
              f["meta"]["record_count"], "works")
    return dests


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--s3-prefix", required=True,
                    help="e.g. s3://YOUR_BUCKET/Access/e1e6")
    args = ap.parse_args()
    prefix = args.s3_prefix.rstrip("/")

    date, chosen, total = pick_parts()
    print("OpenAlex snapshot date:", date, "| staging", len(chosen),
          "parts,", total, "bytes")
    dests = stage(chosen, prefix + "/raw_works")

    spark = SparkSession.builder.appName("bas-e1e6-prep").getOrCreate()
    works = spark.read.parquet(*dests)
    print("WORKS SCHEMA:")
    works.printSchema()

    exploded = works.select(
        F.col("id").alias("work_id"),
        F.explode("authorships").alias("a"))
    fact = exploded.select(
        "work_id",
        F.col("a.author.id").alias("author_id"),
        F.col("a.author.display_name").alias("display_name"),
    ).filter(F.col("author_id").isNotNull())
    fact.write.mode("overwrite").parquet(prefix + "/data/fact_authorships")
    dim = fact.select("author_id", "display_name").distinct()
    dim.write.mode("overwrite").parquet(prefix + "/data/dim_authors")
    meta = works.select(
        F.col("id").alias("work_id"),
        F.col("publication_year")).distinct()
    meta.write.mode("overwrite").parquet(prefix + "/data/works_meta")

    n_fact = fact.count()
    n_dim = dim.count()
    print("FACT authorships rows:", n_fact)
    print("DIM authors rows:", n_dim)
    print("TOP 30 AUTHOR FREQUENCIES (the natural skew):")
    top = (fact.groupBy("author_id").count()
           .orderBy(F.desc("count")).limit(30).collect())
    for row in top:
        print("  author", row["author_id"], "works:", row["count"])
    summary = {
        "step": "prep",
        "openalex_snapshot_date": date,
        "parts": [{"url": f["url"],
                   "bytes": f["meta"]["content_length"],
                   "works": f["meta"]["record_count"]} for f in chosen],
        "staged_bytes": total,
        "fact_rows": n_fact,
        "dim_rows": n_dim,
        "top_author_counts": [r["count"] for r in top],
    }
    from common import write_json
    write_json(spark, summary, prefix + "/output/prep_summary.json")
    spark.stop()


if __name__ == "__main__":
    main()
