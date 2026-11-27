"""STEP 5 - A9 (end-biased sampling, Estan & Naughton 2006) at 100M.

The paper's hole: A9 was only ever run at local scale. This step
runs the study's Experiment A estimator (code/classical_arms.py,
ported line-for-line to the Python standard library - the original
uses numpy only as a vectoriser; K = 20,000 keys, so plain Python
is exact) against the 100M synthetic corpus.

Inputs: the synthetic parquet the Nov 23 EMR run left in S3
(--synthetic-prefix, default s3://YOUR_BUCKET/Access/synthetic, files
data/fact.parquet, data/fact2.parquet). Spark computes the per-key
counts; everything after that is the A9 estimator on the driver.

Ground truth for scoring already exists (Nov 23 EMR run, threshold
T = 2,000,000 = 0.02 x 100M). This step writes A9's predicted loads,
verdicts at that threshold, and the packet's serialized byte size -
the same quantities Experiment A reported at local scale.
"""
import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import write_json

K = 20_000
P = 128
TARGET_RETAINED = 1024
HSEED = 0
MASK = (1 << 64) - 1
T_100M = 2_000_000  # 0.02 x 100M, the Nov 23 verifier threshold


def splitmix64(z):
    z = (z + 1) & MASK
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK
    return z ^ (z >> 31)


def sampling_hash(v):
    return splitmix64((v + HSEED) & MASK) / float(1 << 64)


def expected_retained(sorted_desc, total, t):
    head = 0
    for c in sorted_desc:
        if c >= t:
            head += 1
        else:
            break
    tail_mass = total - sum(sorted_desc[:head])
    return head + tail_mass / t


def find_threshold(counts):
    total = sum(counts)
    if total == 0:
        return 1
    srt = sorted(counts, reverse=True)
    lo, hi = 1, total
    while lo < hi:
        mid = (lo + hi) // 2
        if expected_retained(srt, total, mid) <= TARGET_RETAINED:
            hi = mid
        else:
            lo = mid + 1
    return lo


def build_sample(counts):
    """Returns (threshold, retained {key: exact count}, ht list)."""
    t = find_threshold(counts)
    retained = {}
    ht = [0.0] * len(counts)
    for v, c in enumerate(counts):
        if c == 0:
            continue
        if c >= t:
            retained[v] = c
            ht[v] = float(c)
        elif sampling_hash(v) <= c / t:
            retained[v] = c
            ht[v] = float(t)
    return t, retained, ht


def f_contributions(ta, tb, ret_a, ret_b, counts_a, counts_b):
    """Estan & Naughton Eq. 1 per-key join-output estimates."""
    out = [0.0] * len(counts_a)
    for v in set(ret_a) & set(ret_b):
        a, b = counts_a[v], counts_b[v]
        a_hi, b_hi = a >= ta, b >= tb
        if a_hi and b_hi:
            out[v] = a * b
        elif b_hi:
            out[v] = ta * b
        elif a_hi:
            out[v] = a * tb
        else:
            out[v] = max(ta * b, tb * a)
    return out


def max_partition_load(counts):
    loads = [0.0] * P
    for v, c in enumerate(counts):
        if c:
            loads[splitmix64(v) % P] += c
    return max(loads)


def dense_counts(spark, df, col):
    rows = df.groupBy(col).count().collect()
    out = [0] * K
    for r in rows:
        if r[0] is not None and 0 <= int(r[0]) < K:
            out[int(r[0])] = int(r[1])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--s3-prefix", required=True)
    ap.add_argument("--synthetic-prefix",
                    default="s3://YOUR_BUCKET/Access/synthetic")
    args = ap.parse_args()
    prefix = args.s3_prefix.rstrip("/")
    syn = args.synthetic_prefix.rstrip("/")

    spark = SparkSession.builder.appName("bas-e1e6-a9-100m").getOrCreate()
    fact = spark.read.parquet(syn + "/data/fact.parquet")
    fact2 = spark.read.parquet(syn + "/data/fact2.parquet")
    print("FACT SCHEMA:"); fact.printSchema()
    print("FACT2 SCHEMA:"); fact2.printSchema()

    counts = {}
    for col in ("kA", "kB", "kC", "kN"):
        counts["fact_" + col] = dense_counts(spark, fact, col)
    for col in ("kA", "kB"):
        counts["fact2_" + col] = dense_counts(spark, fact2, col)
    null_count = fact.filter(F.col("kN").isNull()).count()
    counts["null_count"] = null_count

    samples = {}
    packet = {"hash_seed": HSEED, "target_retained": TARGET_RETAINED,
              "fact": {}, "fact2": {}}
    for col in ("kA", "kB", "kC", "kN"):
        t, ret, ht = build_sample(counts["fact_" + col])
        samples[("fact", col)] = (t, ret, ht)
        packet["fact"][col] = {
            "threshold": t, "total": sum(counts["fact_" + col]),
            "keys": sorted(ret), "counts": [ret[k] for k in sorted(ret)]}
    for col in ("kA", "kB"):
        t, ret, ht = build_sample(counts["fact2_" + col])
        samples[("fact2", col)] = (t, ret, ht)
        packet["fact2"][col] = {
            "threshold": t, "total": sum(counts["fact2_" + col]),
            "keys": sorted(ret), "counts": [ret[k] for k in sorted(ret)]}
    t_kn = samples[("fact", "kN")][0]
    if null_count >= t_kn:
        null_ht, null_retained = float(null_count), True
    else:
        null_retained = sampling_hash(K) <= null_count / t_kn
        null_ht = float(t_kn) if null_retained else 0.0
    packet["null"] = {"count": null_count, "threshold": t_kn,
                      "retained": null_retained}
    packet_bytes = len(json.dumps(packet).encode("utf-8"))

    def ht_of(tbl, col):
        return samples[(tbl, col)][2]

    loads = {}
    loads["G_A"] = max(ht_of("fact", "kA"))
    loads["G_B"] = max(ht_of("fact", "kB"))
    loads["G_C"] = max(ht_of("fact", "kC"))
    loads["G_N"] = max(max(ht_of("fact", "kN")), null_ht)
    loads["W_A"] = loads["G_A"]
    loads["J_A"] = max_partition_load(ht_of("fact", "kA"))
    loads["J_B"] = max_partition_load(ht_of("fact", "kB"))
    kn_ht = list(ht_of("fact", "kN")) + [null_ht]  # NULL = pseudo-key K
    loads_jn = [0.0] * P
    for v, c in enumerate(kn_ht):
        if c:
            loads_jn[splitmix64(v) % P] += c
    loads["J_N"] = max(loads_jn)
    for pid, col in (("F_A", "kA"), ("F_B", "kB")):
        ta, ret_a, _ = samples[("fact", col)]
        tb, ret_b, _ = samples[("fact2", col)]
        contrib = f_contributions(ta, tb, ret_a, ret_b,
                                  counts["fact_" + col],
                                  counts["fact2_" + col])
        loads[pid] = max_partition_load(contrib)

    result = {
        "step": "a9_100m", "arm": "A9", "scale": "100M",
        "threshold_T": T_100M,
        "packet_bytes": packet_bytes,
        "thresholds": {("%s_%s" % k): v[0] for k, v in samples.items()},
        "retained_counts": {("%s_%s" % k): len(v[1])
                            for k, v in samples.items()},
        "predictions": {
            pid: {"pred_load": load,
                  "verdict": "FAIL" if load > T_100M else "PASS"}
            for pid, load in loads.items()},
        "note": ("D_A (distinct) excluded: diagnostic only, as in "
                 "the study. J_N treats the NULL group per the "
                 "pre-registration (exact-head pseudo-key)."),
        "status": "OK",
    }
    print(json.dumps(result, indent=1))
    write_json(spark, result, prefix + "/output/a9_100m.json")
    # CSV in the arm-output shape (arm, pipeline, pred_load, verdict,
    # packet_bytes) for direct comparison with the local-scale files.
    lines = ["arm,pipeline,pred_load,verdict,packet_bytes"]
    for pid in sorted(loads):
        lines.append("A9,%s,%.1f,%s,%d" % (
            pid, loads[pid],
            "FAIL" if loads[pid] > T_100M else "PASS", packet_bytes))
    spark.sparkContext.parallelize(lines, 1).saveAsTextFile(
        prefix + "/output/a9_100m_csv.tmp")
    spark.stop()


if __name__ == "__main__":
    main()
