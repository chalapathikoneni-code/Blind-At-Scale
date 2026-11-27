"""Generate one synthetic 'production' dataset (PREREGISTRATION.md S.1).

Counts are drawn per key column from the family's popularity law
(multinomial -- distributionally identical to iid row draws). The fact /
fact2 / dim tables are then MATERIALISED as parquet, and ground-truth
per-key counts are re-derived by executing SQL in DuckDB on those tables
and asserted equal to the drawn counts. F (fact-fact) per-key output
counts are the exact product of the validated per-key counts
(Amendment 1b). Exact distinct subkeys per kA key come from DuckDB
COUNT(DISTINCT ...) on the materialised table.
"""
import argparse, json, time
import numpy as np
import duckdb

from common import K

NULL_SENTINEL = -1


def law_pmf(family: str, col: str) -> np.ndarray:
    """Popularity pmf over key ids 0..K-1 (key 0 = hottest where a
    ranking exists). z for lognormal is fixed across all datasets."""
    if family == "zipf":
        exp = {"kA": 1.0, "kB": 0.6, "kC": 0.0, "kN": 1.0}[col]
        ranks = np.arange(1, K + 1, dtype=np.float64)
        w = ranks ** (-exp)
    elif family == "lognormal":
        sigma = {"kA": 1.6, "kB": 0.9, "kC": 0.15, "kN": 1.6}[col]
        z = np.random.default_rng(12345).standard_normal(K)
        w = np.exp(sigma * z)
    elif family == "mixed":
        w = np.empty(K, dtype=np.float64)
        if col in ("kA", "kN"):
            w[:20] = 0.35 / 20
            w[20:520] = 0.35 / 500
            w[520:] = 0.30 / (K - 520)
        elif col == "kB":
            w[:500] = 0.40 / 500
            w[500:] = 0.60 / (K - 500)
        else:  # kC
            w[:] = 1.0 / K
    else:
        raise ValueError(family)
    return w / w.sum()


def null_prob(family: str) -> float:
    return {"zipf": 0.04, "lognormal": 0.04, "mixed": 0.06}[family]


def draw_counts(rng, family, col, n):
    return rng.multinomial(n, law_pmf(family, col)).astype(np.int64)


def rows_from_counts(rng, counts):
    rows = np.repeat(np.arange(K, dtype=np.int32), counts)
    rng.shuffle(rows)
    return rows


def write_parquet(con, df, path):
    con.register("_w", df)
    con.execute(f"COPY (SELECT * FROM _w) TO '{path}' (FORMAT PARQUET)")
    con.unregister("_w")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--family", required=True)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--n-fact", type=int, required=True)
    ap.add_argument("--n-fact2", type=int, required=True)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    import os
    os.makedirs(args.outdir, exist_ok=True)
    t0 = time.time()
    rng = np.random.default_rng(args.seed)
    rng2 = np.random.default_rng(args.seed + 777)
    fam, n, n2 = args.family, args.n_fact, args.n_fact2
    pnull = null_prob(fam)

    fact_counts, fact2_counts = {}, {}
    for col in ("kA", "kB", "kC"):
        fact_counts[col] = draw_counts(rng, fam, col, n)
    nonnull = int(round(n * (1 - pnull)))
    fact_counts["kN"] = draw_counts(rng, fam, "kN", nonnull)
    null_count = int(n - nonnull)
    for col in ("kA", "kB"):
        fact2_counts[col] = draw_counts(rng2, fam, col, n2)

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
        "s": rng.integers(0, 100_000, size=n, dtype=np.int32),
        "v": rng.random(n, dtype=np.float32),
    })
    fact2 = pd.DataFrame({
        "kA": rows_from_counts(rng2, fact2_counts["kA"]),
        "kB": rows_from_counts(rng2, fact2_counts["kB"]),
    })
    dim = pd.DataFrame({"dim_key": np.arange(K, dtype=np.int32),
                        "payload": rng.integers(0, 1000, size=K, dtype=np.int32)})
    gen_s = time.time() - t0

    con = duckdb.connect(os.path.join(args.outdir, "data.duckdb"))
    con.execute("SET memory_limit='4GB'")
    fp = os.path.join(args.outdir, "fact.parquet")
    f2p = os.path.join(args.outdir, "fact2.parquet")
    dp = os.path.join(args.outdir, "dim.parquet")
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
        os.path.join(args.outdir, "counts.npz"),
        **{f"fact_{c}": gt_fact[c] for c in ("kA", "kB", "kC", "kN")},
        **{f"fact2_{c}": gt_fact2[c] for c in ("kA", "kB")},
        distinct_A=distinct_A, null_count=np.int64(null_count))
    with open(os.path.join(args.outdir, "validation.json"), "w") as f:
        json.dump({"checks_max_abs_diff": checks, "all_exact": ok}, f, indent=1)
    with open(os.path.join(args.outdir, "meta.json"), "w") as f:
        json.dump({"family": fam, "seed": args.seed, "n_fact": n, "n_fact2": n2,
                   "p_null": pnull, "gen_seconds": round(gen_s, 2),
                   "ground_truth_sql_seconds": round(gt_s, 2),
                   "validation_ok": ok}, f, indent=1)
    print(f"{fam} seed={args.seed} n={n}: validation_ok={ok} gen={gen_s:.1f}s gt={gt_s:.1f}s")
    if not ok:
        raise SystemExit(f"VALIDATION FAILED: {checks}")


if __name__ == "__main__":
    main()
