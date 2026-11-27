"""Generate one near-threshold dataset (PREREGISTRATION_EXP_A.md S.7).

Family "near": Zipf(s) popularity law with a tunable exponent s for
kA, kB and kN (kC uniform; p_null = 0.04), fact 10M / fact2 6M /
dim 20k keys. The exponent s moves the max-group and join-partition
loads across T = 200,000 (varying N cannot: T = 0.02 * N scales with
N). Structure mirrors code/gen_dataset.py exactly: multinomial counts
from the law, materialisation as parquet, ground truth by DuckDB SQL
execution, validation against drawn counts (all_exact required -- a
dataset failing validation is discarded, never used). Same
counts.npz / meta.json / validation.json format; meta records s and
family "near". Existing gen_dataset.py is untouched; only its
rows_from_counts / write_parquet helpers and NULL_SENTINEL are reused.

Usage:
    python gen_near_threshold.py --s 0.72 --seed 101 --outdir DIR
"""
import argparse, json, os, time
import numpy as np
import duckdb

from common import K
from gen_dataset import rows_from_counts, write_parquet, NULL_SENTINEL

P_NULL = 0.04


def zipf_pmf(s):
    ranks = np.arange(1, K + 1, dtype=np.float64)
    w = ranks ** (-s)
    return w / w.sum()


def uniform_pmf():
    return np.full(K, 1.0 / K, dtype=np.float64)


def generate(s, seed, outdir, n_fact=10_000_000, n_fact2=6_000_000):
    os.makedirs(outdir, exist_ok=True)
    t0 = time.time()
    rng = np.random.default_rng(seed)
    rng2 = np.random.default_rng(seed + 777)
    pmf = zipf_pmf(s)
    uni = uniform_pmf()

    fact_counts, fact2_counts = {}, {}
    fact_counts["kA"] = rng.multinomial(n_fact, pmf).astype(np.int64)
    fact_counts["kB"] = rng.multinomial(n_fact, pmf).astype(np.int64)
    fact_counts["kC"] = rng.multinomial(n_fact, uni).astype(np.int64)
    nonnull = int(round(n_fact * (1 - P_NULL)))
    fact_counts["kN"] = rng.multinomial(nonnull, pmf).astype(np.int64)
    null_count = int(n_fact - nonnull)
    fact2_counts["kA"] = rng2.multinomial(n_fact2, pmf).astype(np.int64)
    fact2_counts["kB"] = rng2.multinomial(n_fact2, pmf).astype(np.int64)

    # --- materialise fact ---
    import pandas as pd
    kn_rows = np.concatenate([
        rows_from_counts(rng, fact_counts["kN"]),
        np.full(null_count, NULL_SENTINEL, dtype=np.int32)]).astype(np.int32)
    rng.shuffle(kn_rows)  # interleave NULL rows among kN values
    fact = pd.DataFrame({
        "kA": rows_from_counts(rng, fact_counts["kA"]),
        "kB": rows_from_counts(rng, fact_counts["kB"]),
        "kC": rows_from_counts(rng, fact_counts["kC"]),
        "kN": kn_rows,
        "s": rng.integers(0, 100_000, size=n_fact, dtype=np.int32),
        "v": rng.random(n_fact, dtype=np.float32),
    })
    fact2 = pd.DataFrame({
        "kA": rows_from_counts(rng2, fact2_counts["kA"]),
        "kB": rows_from_counts(rng2, fact2_counts["kB"]),
    })
    dim = pd.DataFrame({"dim_key": np.arange(K, dtype=np.int32),
                        "payload": rng.integers(0, 1000, size=K, dtype=np.int32)})
    gen_s = time.time() - t0

    con = duckdb.connect(os.path.join(outdir, "data.duckdb"))
    con.execute("SET memory_limit='4GB'")
    fp = os.path.join(outdir, "fact.parquet")
    f2p = os.path.join(outdir, "fact2.parquet")
    dp = os.path.join(outdir, "dim.parquet")
    write_parquet(con, fact, fp)
    write_parquet(con, fact2, f2p)
    write_parquet(con, dim, dp)
    del fact, fact2, dim
    con.execute(f"CREATE VIEW fact AS SELECT * FROM '{fp}'")
    con.execute(f"CREATE VIEW fact2 AS SELECT * FROM '{f2p}'")
    con.execute(f"CREATE VIEW dim AS SELECT * FROM '{dp}'")

    # --- ground truth by SQL execution + validation against drawn counts ---
    t1 = time.time()
    checks = {}
    gt_fact = {}
    for col in ("kA", "kB", "kC", "kN"):
        df = con.execute(f"SELECT {col} AS k, COUNT(*) AS c FROM fact GROUP BY {col}").df()
        arr = np.zeros(K, dtype=np.int64)
        nn = df[df["k"] >= 0]
        arr[nn["k"].to_numpy(dtype=np.int64)] = nn["c"].to_numpy(dtype=np.int64)
        gt_fact[col] = arr
        checks[f"counts_{col}"] = int(np.abs(arr - fact_counts[col]).max())
        if col == "kN":
            nrow = df[df["k"] == NULL_SENTINEL]
            checks["null_count"] = int(abs((int(nrow["c"].iloc[0]) if len(nrow) else 0) - null_count))
    gt_fact2 = {}
    for col in ("kA", "kB"):
        df = con.execute(f"SELECT {col} AS k, COUNT(*) AS c FROM fact2 GROUP BY {col}").df()
        arr = np.zeros(K, dtype=np.int64)
        arr[df["k"].to_numpy(dtype=np.int64)] = df["c"].to_numpy(dtype=np.int64)
        gt_fact2[col] = arr
        checks[f"counts_fact2_{col}"] = int(np.abs(arr - fact2_counts[col]).max())
    for col in ("kA", "kB", "kN"):
        df = con.execute(
            f"SELECT f.{col} AS k, COUNT(*) AS c FROM fact f JOIN dim d "
            f"ON f.{col} = d.dim_key GROUP BY f.{col}").df()
        arr = np.zeros(K, dtype=np.int64)
        arr[df["k"].to_numpy(dtype=np.int64)] = df["c"].to_numpy(dtype=np.int64)
        checks[f"join_counts_{col}"] = int(np.abs(arr - gt_fact[col]).max())
    df = con.execute("SELECT kA AS k, COUNT(DISTINCT s) AS d FROM fact GROUP BY kA").df()
    distinct_A = np.zeros(K, dtype=np.int64)
    distinct_A[df["k"].to_numpy(dtype=np.int64)] = df["d"].to_numpy(dtype=np.int64)
    gt_s = time.time() - t1
    con.close()

    ok = all(v == 0 for v in checks.values())
    np.savez_compressed(
        os.path.join(outdir, "counts.npz"),
        **{f"fact_{c}": gt_fact[c] for c in ("kA", "kB", "kC", "kN")},
        **{f"fact2_{c}": gt_fact2[c] for c in ("kA", "kB")},
        distinct_A=distinct_A, null_count=np.int64(null_count))
    with open(os.path.join(outdir, "validation.json"), "w") as f:
        json.dump({"checks_max_abs_diff": checks, "all_exact": ok}, f, indent=1)
    with open(os.path.join(outdir, "meta.json"), "w") as f:
        json.dump({"family": "near", "zipf_s": s, "seed": seed,
                   "n_fact": n_fact, "n_fact2": n_fact2,
                   "p_null": P_NULL, "gen_seconds": round(gen_s, 2),
                   "ground_truth_sql_seconds": round(gt_s, 2),
                   "validation_ok": ok}, f, indent=1)
    print(f"near s={s} seed={seed} n={n_fact}: validation_ok={ok} "
          f"gen={gen_s:.1f}s gt={gt_s:.1f}s")
    if not ok:
        raise SystemExit(f"VALIDATION FAILED: {checks}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--s", type=float, required=True)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    generate(args.s, args.seed, args.outdir)


if __name__ == "__main__":
    main()
