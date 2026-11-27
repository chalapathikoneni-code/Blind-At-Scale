"""Generate one NON-ALIGNED fact-fact dataset (PREREGISTRATION.md
Amendment 5, Section A5.2, locked 2026-11-06 12:15 UTC).

This is a copy/adaptation of gen_dataset.py with ONE change: before
drawing fact2 counts for the pair columns kA and kB, that column's
popularity pmf is permuted across key ids,

    pmf2[k] = pmf[pi(k)],

with an independent permutation pi per pair column, drawn from a numpy
default_rng seeded dataset_seed + 31337 (kA) / dataset_seed + 31338
(kB). The permutation applies to the PMF (popularity); fact2 counts are
then drawn multinomially from the permuted pmf with the dataset's
fact2 rng stream (seed + 777, as in gen_dataset.py). All other rng
usage mirrors gen_dataset.py exactly, so the fact counts of a
non-aligned dataset are bit-identical to the aligned dataset's fact
counts for the same family/seed at the same sizes (verified
externally against data/<family>_s<seed>/counts.npz, and by
score_nonaligned.py).

Levels:
- NA1 (partial misalignment, phi = 0.5): pi permutes a uniformly
  random subset of phi * K of the K key ids among themselves (subset
  chosen via the same per-column rng, then permuted via that rng),
  identity elsewhere.
- NA2 (full misalignment, phi = 1.0): pi is a uniform random
  permutation of all K key ids.

Everything else is identical to gen_dataset.py: sizes fact
10,000,000 / fact2 6,000,000, K = 20,000, materialisation of
fact/fact2/dim parquet, and DuckDB ground-truth cross-validation
(per-column counts including fact2 vs the drawn permuted counts,
join counts, exact distinct_A). If any validation check is nonzero
the script exits nonzero (dataset must be discarded/regenerated).

meta.json carries the gen_dataset.py fields plus: level, phi, and
the realised alignment -- Spearman rho (scipy.stats.spearmanr)
between the fact and fact2 per-key count vectors for kA and for kB.
"""
import argparse, json, time
import numpy as np
import duckdb
from scipy import stats

from common import K
from gen_dataset import law_pmf, null_prob, rows_from_counts, write_parquet

NULL_SENTINEL = -1

PHI = {"NA1": 0.5, "NA2": 1.0}
PI_SEED_OFFSET = {"kA": 31337, "kB": 31338}


def make_permutation(level: str, seed: int, col: str) -> np.ndarray:
    """pi as an index array: pmf2[k] = pmf[pi[k]]."""
    rng = np.random.default_rng(seed + PI_SEED_OFFSET[col])
    if level == "NA2":
        return rng.permutation(K)
    if level == "NA1":
        n_sub = int(round(PHI["NA1"] * K))
        subset = rng.choice(K, size=n_sub, replace=False)
        permuted = rng.permutation(subset)
        pi = np.arange(K, dtype=np.int64)
        pi[subset] = permuted
        return pi
    raise ValueError(level)


def draw_counts_permuted(rng, family, col, n, pi):
    pmf = law_pmf(family, col)
    pmf2 = pmf[pi]
    return rng.multinomial(n, pmf2).astype(np.int64)


def draw_counts(rng, family, col, n):
    return rng.multinomial(n, law_pmf(family, col)).astype(np.int64)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--family", required=True)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--level", required=True, choices=("NA1", "NA2"))
    ap.add_argument("--n-fact", type=int, default=10_000_000)
    ap.add_argument("--n-fact2", type=int, default=6_000_000)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    import os
    os.makedirs(args.outdir, exist_ok=True)
    t0 = time.time()
    # --- rng streams: identical seeds / order to gen_dataset.py ---
    rng = np.random.default_rng(args.seed)
    rng2 = np.random.default_rng(args.seed + 777)
    fam, n, n2 = args.family, args.n_fact, args.n_fact2
    level = args.level
    pnull = null_prob(fam)

    fact_counts, fact2_counts = {}, {}
    for col in ("kA", "kB", "kC"):
        fact_counts[col] = draw_counts(rng, fam, col, n)
    nonnull = int(round(n * (1 - pnull)))
    fact_counts["kN"] = draw_counts(rng, fam, "kN", nonnull)
    null_count = int(n - nonnull)
    # THE ONE CHANGE vs gen_dataset.py: permuted fact2 pmf per pair col.
    for col in ("kA", "kB"):
        pi = make_permutation(level, args.seed, col)
        fact2_counts[col] = draw_counts_permuted(rng2, fam, col, n2, pi)

    # --- materialise fact (identical to gen_dataset.py) ---
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
    # realised alignment: Spearman rho between fact and fact2 per-key
    # count vectors (ground-truth vectors; identical to drawn counts
    # when validation is exact).
    rho_kA = float(stats.spearmanr(gt_fact["kA"], gt_fact2["kA"]).statistic)
    rho_kB = float(stats.spearmanr(gt_fact["kB"], gt_fact2["kB"]).statistic)
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
                   "validation_ok": ok,
                   "level": level, "phi": PHI[level],
                   "spearman_rho_kA": rho_kA, "spearman_rho_kB": rho_kB}, f, indent=1)
    print(f"{level} {fam} seed={args.seed} n={n}: validation_ok={ok} "
          f"rho_kA={rho_kA:.4f} rho_kB={rho_kB:.4f} gen={gen_s:.1f}s gt={gt_s:.1f}s")
    if not ok:
        raise SystemExit(f"VALIDATION FAILED: {checks}")


if __name__ == "__main__":
    main()
