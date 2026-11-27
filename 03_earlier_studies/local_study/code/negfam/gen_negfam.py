"""Generate one negative-family dataset (PREREGISTRATION.md Amendment 4).

Int columns are drawn in numpy (distributionally exact); the fact table
with the VARCHAR payload / sprefix columns is materialised by DuckDB
(repeat() of the drawn length), written as ZSTD parquet. Ground-truth
per-key row counts and payload-byte totals are re-derived by SQL on the
materialised tables and validated against the drawn arrays.
"""
import argparse, json, os, time
import numpy as np
import duckdb

K = 20_000
P_WIDE = 0.02


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--n-fact", type=int, default=5_000_000)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    t0 = time.time()
    rng = np.random.default_rng(args.seed)
    n = args.n_fact

    # key counts: Zipf s=0.6 over ranks (key 0 hottest), as main gen kB law
    ranks = np.arange(1, K + 1, dtype=np.float64)
    pmf = ranks ** (-0.6)
    pmf /= pmf.sum()
    counts = rng.multinomial(n, pmf).astype(np.int64)
    k_arr = np.repeat(np.arange(K, dtype=np.int32), counts)
    rng.shuffle(k_arr)

    # value attribute + payload lengths (independent of k)
    cat = (rng.random(n) < P_WIDE).astype(np.int32)  # 1 = WIDE, 0 = NORMAL
    plen = rng.integers(20, 101, size=n, dtype=np.int32)
    n_wide = int(cat.sum())
    wide_idx = np.flatnonzero(cat == 1)
    plen[wide_idx] = rng.integers(5000, 20001, size=n_wide, dtype=np.int32)
    pcode = rng.integers(0, 10_000, size=n, dtype=np.int32)
    pcode[wide_idx] = -1

    total_len = int(plen.astype(np.int64).sum())
    wide_len = int(plen[wide_idx].astype(np.int64).sum())
    sg = np.where(cat == 1, 10_000, pcode)  # sprefix group ids; 10000 = 'WIDE'
    sg_counts = np.bincount(sg, minlength=10_001).astype(np.int64)
    gen_s = time.time() - t0

    import pandas as pd
    ints = pd.DataFrame({
        "rid": np.arange(n, dtype=np.int32),
        "k": k_arr, "cat": cat, "plen": plen, "pcode": pcode,
    })
    con = duckdb.connect()
    con.execute("PRAGMA threads=2")
    con.execute("SET memory_limit='2GB'")
    ip = os.path.join(args.outdir, "fact_int.parquet")
    con.register("_ints", ints)
    con.execute(f"COPY (SELECT * FROM _ints) TO '{ip}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.unregister("_ints")
    del ints, k_arr, cat, pcode

    fp = os.path.join(args.outdir, "fact.parquet")
    dp = os.path.join(args.outdir, "dim.parquet")
    con.execute(f"CREATE VIEW ints AS SELECT * FROM '{ip}'")
    t1 = time.time()
    con.execute(
        f"COPY (SELECT rid, k, cat, "
        f"CASE WHEN cat = 1 THEN repeat('w', plen) ELSE repeat('n', plen) END AS payload, "
        f"CASE WHEN cat = 1 THEN 'WIDE' ELSE 'p' || CAST(pcode AS VARCHAR) END AS sprefix "
        f"FROM ints) TO '{fp}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.execute(
        f"COPY (SELECT CAST(i AS INTEGER) AS dim_key, repeat('d', 48) AS dpayload "
        f"FROM range({K}) AS t(i)) TO '{dp}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    mat_s = time.time() - t1

    # validation by SQL execution on the materialised tables
    con.execute(f"CREATE VIEW fact AS SELECT * FROM '{fp}'")
    con.execute(f"CREATE VIEW dim AS SELECT * FROM '{dp}'")
    df = con.execute("SELECT k, COUNT(*) AS c FROM fact GROUP BY k").df()
    arr = np.zeros(K, dtype=np.int64)
    arr[df["k"].to_numpy(dtype=np.int64)] = df["c"].to_numpy(dtype=np.int64)
    tot_sql, wide_sql = con.execute(
        "SELECT SUM(LENGTH(payload)), SUM(LENGTH(payload)) FILTER (cat = 1) FROM fact"
    ).fetchone()
    wgrp = con.execute(
        "SELECT SUM(LENGTH(payload)) FROM fact WHERE sprefix = 'WIDE'").fetchone()[0]
    jtot = con.execute(
        "SELECT SUM(LENGTH(f.payload) + LENGTH(d.dpayload)) FROM fact f "
        "JOIN dim d ON f.k = d.dim_key").fetchone()[0]
    checks = {
        "counts_k_max_abs_diff": int(np.abs(arr - counts).max()),
        "total_len_diff": int(int(tot_sql) - total_len),
        "wide_len_diff": int(int(wide_sql) - wide_len),
        "wide_sprefix_len_diff": int(int(wgrp) - wide_len),
        "join_total_bytes_diff": int(int(jtot) - (total_len + 48 * n)),
    }
    ok = all(v == 0 for v in checks.values())
    con.close()

    np.savez_compressed(
        os.path.join(args.outdir, "counts.npz"),
        fact_k=counts, plen_sum=np.int64(total_len),
        wide_len=np.int64(wide_len), n_wide=np.int64(n_wide))
    # payload-length / sprefix marginals for arm A4-NF (lengths only, int form)
    np.savez_compressed(
        os.path.join(args.outdir, "marginals.npz"), plen=plen, sg_counts=sg_counts)
    with open(os.path.join(args.outdir, "validation.json"), "w") as f:
        json.dump({"checks": checks, "all_exact": ok}, f, indent=1)
    with open(os.path.join(args.outdir, "meta.json"), "w") as f:
        json.dump({"family": "negfam", "seed": args.seed, "n_fact": n, "K": K,
                   "key_law": "zipf_s0.6", "p_wide": P_WIDE, "n_wide": n_wide,
                   "total_payload_bytes": total_len,
                   "gen_seconds": round(gen_s, 2),
                   "materialise_seconds": round(mat_s, 2),
                   "validation_ok": ok}, f, indent=1)
    print(f"negfam seed={args.seed} n={n} n_wide={n_wide} total_len={total_len} "
          f"validation_ok={ok} gen={gen_s:.1f}s mat={mat_s:.1f}s")
    if not ok:
        raise SystemExit(f"VALIDATION FAILED: {checks}")


if __name__ == "__main__":
    main()
