"""Blind at Scale E1/E6 - shared helpers. Python standard library only.

Imported by the Spark jobs via --py-files. No numpy, no pandas, no pip.
"""
import json
import subprocess
import zlib

PARTITIONS = 128
THRESH_REL = 2.0        # FAIL if max partition load > 2.0 x mean load
THRESH_ABS_FRAC = 0.02  # FAIL if max partition load > 0.02 x total rows

# Safety guard (recovery patch): a case whose SYNOPSIS prediction
# exceeds either cap is recorded as SKIPPED_GUARD with its exact
# predicted values and never materialized on the cluster.
MAX_OUTPUT_ROWS = 2_000_000_000   # predicted total output rows
MAX_LOAD_ROWS = 25_000_000        # predicted max partition load (rows)


def count_per_partition(rdd):
    """Stream each partition's row count without materializing it.

    Replaces rdd.glom().map(len).collect(), which pulls an entire
    partition into memory (the skewed-self-join OOM/disk-saturation
    that killed Step 4 and Step 6). mapPartitionsWithIndex consumes
    the iterator and keeps only a running integer per partition.
    Returns per-partition counts in partition-index order.
    """
    kv = rdd.mapPartitionsWithIndex(
        lambda i, it: iter([(i, sum(1 for _ in it))])).collect()
    kv.sort()
    return [c for _, c in kv]


def write_json(spark, obj, s3_path):
    """Write one JSON object to S3 (driver-side, via Hadoop fs)."""
    text = json.dumps(obj, indent=1, sort_keys=True)
    rdd = spark.sparkContext.parallelize([text], 1)
    tmp = s3_path + ".tmpdir"
    rdd.saveAsTextFile(tmp)
    # Move the single part file to the final name with hadoop fs.
    subprocess.run(
        ["hadoop", "fs", "-mv", tmp + "/part-00000", s3_path],
        check=True)
    subprocess.run(["hadoop", "fs", "-rm", "-r", tmp], check=False)
    print("WROTE", s3_path)


def verdict(max_load, total_rows):
    """Study threshold convention: both rules, reported side by side."""
    mean = total_rows / float(PARTITIONS)
    rel_limit = THRESH_REL * mean
    abs_limit = THRESH_ABS_FRAC * total_rows
    return {
        "mean_load": mean,
        "rel_limit_2x_mean": rel_limit,
        "abs_limit_0.02N": abs_limit,
        "verdict_rel": "FAIL" if max_load > rel_limit else "PASS",
        "verdict_abs": "FAIL" if max_load > abs_limit else "PASS",
    }


def partition_loads_from_counts(counts, fanout):
    """Analytic partition loads: assign each key to a partition with
    crc32 (documented approximation of hash partitioning - the event
    log is the authoritative physical measurement), multiply the key
    count by the join fan-out for that key, and sum per partition.

    counts: {key: left_count}; fanout: {key: right_count} or None
    (None means fan-out = left_count itself, i.e. a self-join).
    """
    loads = [0] * PARTITIONS
    for key, c in counts.items():
        f = c if fanout is None else fanout.get(key, 0)
        out = c * f
        if out:
            loads[zlib.crc32(str(key).encode("utf-8")) % PARTITIONS] += out
    return loads


def predict_from_counts(counts, fanout):
    loads = partition_loads_from_counts(counts, fanout)
    total = sum(loads)
    return {"max_load": max(loads), "total_rows": total,
            "partition_loads": loads}
