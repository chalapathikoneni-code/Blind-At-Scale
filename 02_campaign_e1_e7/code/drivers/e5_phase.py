"""E5 single-phase worker (FINAL_EXECUTION_PROTOCOL.md S.5).

Run as a fresh subprocess per (dataset, phase, repetition) by
e5_driver.py. Prints exactly one JSON object on stdout with the phase
timing, the peak-RSS delta measured inside this process
(resource.getrusage RUSAGE_SELF ru_maxrss, Linux KB), and phase
payload fields.

Phases:
  scan      DuckDB GROUP BY per-key counts for fact (kA,kB,kC,kN) and
            fact2 (kA,kB) + COUNT(DISTINCT s) per kA + kN null-sentinel
            count, from the raw parquet; mirrors the counting queries
            in code/gen_dataset.py. Writes the derived counts to
            --out-counts and validates them against counts.npz.
  build     arms.build_synopsis(counts) from --counts.
  serialize json.dumps(synopsis) time + byte length (synopsis rebuilt
            untimed from --counts).
  predict   one arm per invocation (--arm A8|A5|A9): A8/A5 via
            arms.arm_predictions(counts, synopsis rebuilt untimed,
            seed); A9 via classical_arms.predict_A9(counts).
"""

import argparse
import json
import os
import resource
import sys
import time

import numpy as np

K = 20_000
NULL_SENTINEL = -1
FACT_COLS = ("kA", "kB", "kC", "kN")
PAIR_COLS = ("kA", "kB")


def rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def load_counts(path):
    z = np.load(path)
    return {k: z[k] for k in z.files}


def phase_scan(ddir, out_counts):
    import duckdb

    fact_p = os.path.join(ddir, "fact.parquet")
    fact2_p = os.path.join(ddir, "fact2.parquet")
    con = duckdb.connect()
    base = rss_mb()
    t0 = time.perf_counter()
    counts = {}
    for col in FACT_COLS:
        df = con.execute(
            f"SELECT {col} AS k, COUNT(*) AS c FROM read_parquet('{fact_p}') "
            f"GROUP BY {col}"
        ).df()
        arr = np.zeros(K, dtype=np.int64)
        nn = df[df["k"] >= 0]
        arr[nn["k"].to_numpy(dtype=np.int64)] = nn["c"].to_numpy(dtype=np.int64)
        counts[f"fact_{col}"] = arr
        if col == "kN":
            nrow = df[df["k"] == NULL_SENTINEL]
            counts["null_count"] = np.int64(
                int(nrow["c"].iloc[0]) if len(nrow) else 0
            )
    for col in PAIR_COLS:
        df = con.execute(
            f"SELECT {col} AS k, COUNT(*) AS c FROM read_parquet('{fact2_p}') "
            f"GROUP BY {col}"
        ).df()
        arr = np.zeros(K, dtype=np.int64)
        arr[df["k"].to_numpy(dtype=np.int64)] = df["c"].to_numpy(dtype=np.int64)
        counts[f"fact2_{col}"] = arr
    df = con.execute(
        f"SELECT kA AS k, COUNT(DISTINCT s) AS d FROM read_parquet('{fact_p}') "
        "GROUP BY kA"
    ).df()
    distinct = np.zeros(K, dtype=np.int64)
    distinct[df["k"].to_numpy(dtype=np.int64)] = df["d"].to_numpy(dtype=np.int64)
    counts["distinct_A"] = distinct
    elapsed = time.perf_counter() - t0
    peak = rss_mb() - base
    con.close()

    np.savez(out_counts, **counts)

    ref = load_counts(os.path.join(ddir, "counts.npz"))
    diffs = {}
    for key in sorted(ref.keys()):
        diffs[key] = int(np.abs(counts[key].astype(np.int64)
                                - ref[key].astype(np.int64)).max())
    return {
        "seconds": elapsed,
        "peak_mem_mb_delta": peak,
        "validation_max_abs_diff": diffs,
        "validation_exact": all(v == 0 for v in diffs.values()),
    }


def phase_build(counts_path):
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "..", "..", "code"))
    import arms

    counts = load_counts(counts_path)
    base = rss_mb()
    t0 = time.perf_counter()
    syn = arms.build_synopsis(counts)
    elapsed = time.perf_counter() - t0
    return {"seconds": elapsed, "peak_mem_mb_delta": rss_mb() - base,
            "n_syn_cols": len(syn["cols"])}


def phase_serialize(counts_path):
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "..", "..", "code"))
    import arms

    counts = load_counts(counts_path)
    syn = arms.build_synopsis(counts)
    base = rss_mb()
    t0 = time.perf_counter()
    blob = json.dumps(syn)
    elapsed = time.perf_counter() - t0
    return {"seconds": elapsed, "peak_mem_mb_delta": rss_mb() - base,
            "bytes": len(blob)}


def phase_predict(counts_path, arm, seed):
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "..", "..", "code"))
    import arms
    import classical_arms

    counts = load_counts(counts_path)
    if arm in ("A8", "A5"):
        syn = arms.build_synopsis(counts)
        base = rss_mb()
        t0 = time.perf_counter()
        arms.arm_predictions(arm, counts, syn, seed)
        elapsed = time.perf_counter() - t0
    elif arm == "A9":
        base = rss_mb()
        t0 = time.perf_counter()
        classical_arms.predict_A9(counts)
        elapsed = time.perf_counter() - t0
    else:
        raise ValueError(arm)
    return {"seconds": elapsed, "peak_mem_mb_delta": rss_mb() - base}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", required=True,
                    choices=["scan", "build", "serialize", "predict"])
    ap.add_argument("--dataset-dir", required=True)
    ap.add_argument("--counts", default=None,
                    help="counts npz to consume (build/serialize/predict)")
    ap.add_argument("--out-counts", default=None,
                    help="where scan writes its derived counts npz")
    ap.add_argument("--arm", default=None)
    ap.add_argument("--seed", type=int, default=101)
    a = ap.parse_args()

    if a.phase == "scan":
        out = phase_scan(a.dataset_dir, a.out_counts)
    elif a.phase == "build":
        out = phase_build(a.counts)
    elif a.phase == "serialize":
        out = phase_serialize(a.counts)
    else:
        out = phase_predict(a.counts, a.arm, a.seed)
    out["phase"] = a.phase
    if a.arm:
        out["arm"] = a.arm
    print(json.dumps(out))


if __name__ == "__main__":
    main()
