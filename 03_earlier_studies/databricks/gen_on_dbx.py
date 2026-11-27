"""Generate the Blind at Scale zipf dataset ON DATABRICKS, as parquet in a
Unity Catalog volume that the Spark jobs read.

This is an adaptation of pod/gen_on_pod.py (which itself adapts
code/gen_dataset.py, PREREGISTRATION.md S.1) for a Databricks
spark_python_task. Differences from the pod script:

- Output goes to a Unity Catalog volume path (--outdir, e.g.
  /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/zipf_s101) instead of a local dir.
- Also writes counts.npz + counts_summary.json to --counts-out (default:
  the same outdir) so the counts can be downloaded for the local arms run
  (D-SCALE 100M leg) and for the SHA cross-check.
- numpy version is asserted >= 2.5 (the multinomial sampling algorithm
  changed between numpy 1.26 and 2.x; 1.26 draws do NOT reproduce the
  local counts). Exact 2.5.4 is preferred (the local study's version);
  any other >= 2.5 version is recorded and the SHA gate decides.
- The SHA-256 over the six per-key count vectors is computed and recorded
  for EVERY config (not only headline), so the 40M/100M legs have a
  Databricks-side hash the execution phase can compare against a local
  re-derivation from the exported counts.npz. The EXPECTED comparison
  (fail loudly) applies only to the headline config.

REPRODUCIBILITY CONTRACT (same as gen_on_pod.py):
- Same popularity laws (law_pmf copied verbatim), same key space
  K = 20,000, same sizes, same seeds (fact rng = default_rng(seed);
  fact2 rng = default_rng(seed+777)), and the SAME order of multinomial
  draws (fact kA,kB,kC then kN-nonnull; fact2 kA,kB). The per-key COUNT
  VECTORS are therefore bit-identical to the local study's counts.npz
  for zipf/101, given numpy >= 2.5.
- Row ORDER inside the parquet files may differ from the local files
  (shuffle call sequence is not reproduced operation-for-operation).
  Row order does not enter any load definition in PREREGISTRATION.md S.2.
- dim.payload values differ from the local file (drawn here from
  default_rng(seed+999)); payload is never read by any pipeline except
  as join-output width and does not affect any load statistic.
- kN NULLs are encoded as the int32 sentinel -1 in parquet, EXACTLY as
  in the local gen_dataset.py. 4% of fact rows carry kN = -1 for zipf.

VALIDATION (fails the run if violated):
- row counts and null_count exact;
- for the headline config (zipf, seed 101, 10M/6M) a SHA-256 over all six
  per-key count vectors must equal the local counts.npz hash
  (EXPECTED_COUNTS_SHA256), AND the max per-key counts must equal the
  local ground truth:
    max fact kA = 953,047   (G_A ground-truth load)
    max fact kB = 77,009    (G_B)
    max fact kC = 584       (G_C)
    max fact kN (non-null) = 916,064  (G_N; the -1 group is 400,000)
  For any other config these comparisons are skipped with a warning, but
  the SHA is still recorded.

Usage (as a Databricks spark_python_task; see job_defs/):
  gen_on_dbx.py --family zipf --seed 101 --n-fact 10000000
      --n-fact2 6000000
      --outdir /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/zipf_s101
      --counts-out /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/zipf_s101

Outputs in --outdir: fact.parquet/ fact2.parquet/ dim.parquet/
                     meta.json counts_summary.json
Outputs in --counts-out: counts.npz counts_summary.json
  (parquet written as single files -- Spark reads a 1-file dataset fine;
   file-splitting then follows Spark's 128 MB default, documented in
   spark_pipelines.py deviation D6.)

No Spark session is needed (pure numpy + pyarrow). No DuckDB.
"""
import argparse, hashlib, json, os, time
import numpy as np

K = 20_000
NULL_SENTINEL = -1

EXPECTED_HEADLINE = {  # zipf seed 101, local ground truth (see docstring)
    "fact_kA_max": 953_047,
    "fact_kB_max": 77_009,
    "fact_kC_max": 584,
    "fact_kN_max_nonnull": 916_064,
    "null_count": 400_000,
}
# SHA-256 over the six per-key count vectors (int64 LE, in the order
# fact kA,kB,kC,kN, fact2 kA,kB) of the LOCAL counts.npz, computed
# 2026-11-06. The headline run must reproduce this hash exactly.
EXPECTED_COUNTS_SHA256 = "f9f3fa8b29356e870df37e292f755a03d20fdcd016484d79dbcced148dcb528f"
# NUMPY VERSION MATTERS: Generator.multinomial's sampling algorithm
# changed between numpy 1.26 and 2.x -- with numpy 1.26.4 the draws above
# do NOT reproduce the local counts; with numpy 2.5.4 (the local study's
# version) they are bit-identical (both verified 2026-11-06). The job
# environment spec pins numpy==2.5.4; if the hash check fails, fix the
# numpy version, do NOT relax the check.
NUMPY_MIN = (2, 5)
NUMPY_PREFERRED = "2.5.4"


def law_pmf(family: str, col: str) -> np.ndarray:
    """Copied verbatim from code/gen_dataset.py (via gen_on_pod.py)."""
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


def counts_sha256(fact_counts, fact2_counts):
    """SHA-256 over the six per-key count vectors (int64 LE), order
    fact kA,kB,kC,kN, fact2 kA,kB. Same function as gen_on_pod.py."""
    h = hashlib.sha256()
    for col in ("kA", "kB", "kC", "kN"):
        h.update(fact_counts[col].astype("<i8").tobytes())
    for col in ("kA", "kB"):
        h.update(fact2_counts[col].astype("<i8").tobytes())
    return h.hexdigest()


def write_parquet(table_dict, path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.table({name: pa.array(col) for name, col in table_dict.items()})
    pq.write_table(table, path, row_group_size=1_000_000, compression="snappy")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--family", default="zipf")
    ap.add_argument("--seed", type=int, default=101)
    ap.add_argument("--n-fact", type=int, default=10_000_000)
    ap.add_argument("--n-fact2", type=int, default=6_000_000)
    ap.add_argument("--outdir", required=True,
                    help="Unity Catalog volume path for the parquet files")
    ap.add_argument("--counts-out", default=None,
                    help="dir for counts.npz + counts_summary.json "
                         "(default: same as --outdir)")
    args = ap.parse_args()

    # --- numpy version gate: < 2.5 cannot reproduce the draws at all ---
    ver = tuple(int(x) for x in np.__version__.split(".")[:2])
    if ver < NUMPY_MIN:
        raise SystemExit(
            f"numpy {np.__version__} is too old: Generator.multinomial draws "
            f"differ below 2.x and cannot reproduce the local counts. "
            f"Require numpy >= 2.5 (preferred exactly {NUMPY_PREFERRED}).")
    if np.__version__ != NUMPY_PREFERRED:
        print(f"WARNING: numpy is {np.__version__}, not the preferred "
              f"{NUMPY_PREFERRED}; the SHA gate below is the arbiter.")

    os.makedirs(args.outdir, exist_ok=True)
    counts_out = args.counts_out or args.outdir
    os.makedirs(counts_out, exist_ok=True)
    t0 = time.time()
    rng = np.random.default_rng(args.seed)
    rng2 = np.random.default_rng(args.seed + 777)
    fam, n, n2 = args.family, args.n_fact, args.n_fact2
    pnull = null_prob(fam)

    # --- per-key counts: SAME draw order as code/gen_dataset.py ---
    fact_counts, fact2_counts = {}, {}
    for col in ("kA", "kB", "kC"):
        fact_counts[col] = draw_counts(rng, fam, col, n)
    nonnull = int(round(n * (1 - pnull)))
    fact_counts["kN"] = draw_counts(rng, fam, "kN", nonnull)
    null_count = int(n - nonnull)
    for col in ("kA", "kB"):
        fact2_counts[col] = draw_counts(rng2, fam, col, n2)

    # --- materialise rows (order may differ from local; counts do not) ---
    kn_rows = np.concatenate([
        rows_from_counts(rng, fact_counts["kN"]),
        np.full(null_count, NULL_SENTINEL, dtype=np.int32)]).astype(np.int32)
    rng.shuffle(kn_rows)
    fact = {
        "kA": rows_from_counts(rng, fact_counts["kA"]),
        "kB": rows_from_counts(rng, fact_counts["kB"]),
        "kC": rows_from_counts(rng, fact_counts["kC"]),
        "kN": kn_rows,
        "s": rng.integers(0, 100_000, size=n, dtype=np.int32),
        "v": rng.random(n, dtype=np.float32),
    }
    fact2 = {
        "kA": rows_from_counts(rng2, fact2_counts["kA"]),
        "kB": rows_from_counts(rng2, fact2_counts["kB"]),
    }
    rng_dim = np.random.default_rng(args.seed + 999)  # payload only; see docstring
    dim = {"dim_key": np.arange(K, dtype=np.int32),
           "payload": rng_dim.integers(0, 1000, size=K, dtype=np.int32)}

    write_parquet(fact, os.path.join(args.outdir, "fact.parquet"))
    write_parquet(fact2, os.path.join(args.outdir, "fact2.parquet"))
    write_parquet(dim, os.path.join(args.outdir, "dim.parquet"))
    gen_s = time.time() - t0

    # --- counts export (for download + the local arms run) ---
    np.savez_compressed(
        os.path.join(counts_out, "counts.npz"),
        fact_kA=fact_counts["kA"], fact_kB=fact_counts["kB"],
        fact_kC=fact_counts["kC"], fact_kN=fact_counts["kN"],
        fact2_kA=fact2_counts["kA"], fact2_kB=fact2_counts["kB"],
        null_count=np.int64(null_count))
    sha = counts_sha256(fact_counts, fact2_counts)

    summary = {
        "family": fam, "seed": args.seed, "n_fact": n, "n_fact2": n2,
        "p_null": pnull, "null_count": null_count,
        "fact_kA_max": int(fact_counts["kA"].max()),
        "fact_kB_max": int(fact_counts["kB"].max()),
        "fact_kC_max": int(fact_counts["kC"].max()),
        "fact_kN_max_nonnull": int(fact_counts["kN"].max()),
        "fact2_kA_max": int(fact2_counts["kA"].max()),
        "fact2_kB_max": int(fact2_counts["kB"].max()),
        "top5_fact_kA": sorted((int(x) for x in fact_counts["kA"]), reverse=True)[:5],
        "gen_seconds": round(gen_s, 2),
        "numpy_version": np.__version__,
        "numpy_preferred": NUMPY_PREFERRED,
        "counts_sha256": sha,
    }
    headline = (fam == "zipf" and args.seed == 101 and n == 10_000_000 and n2 == 6_000_000)
    if headline:
        bad = {k: (summary[k], v) for k, v in EXPECTED_HEADLINE.items()
               if summary.get(k) != v}
        if sha != EXPECTED_COUNTS_SHA256:
            bad["counts_sha256"] = (sha, EXPECTED_COUNTS_SHA256)
        summary["headline_expected_check"] = "PASS" if not bad else f"FAIL: {bad}"
        if bad:
            raise SystemExit(f"VALIDATION FAILED vs local ground truth: {bad} "
                             f"(numpy={np.__version__}; see EXPECTED_COUNTS_SHA256 note)")
    else:
        summary["headline_expected_check"] = (
            "SKIPPED (non-headline config; counts_sha256 recorded for "
            "the execution-phase local re-derivation check)")
    with open(os.path.join(counts_out, "counts_summary.json"), "w") as f:
        json.dump(summary, f, indent=1)
    with open(os.path.join(args.outdir, "meta.json"), "w") as f:
        json.dump({k: summary[k] for k in
                   ("family", "seed", "n_fact", "n_fact2", "p_null", "gen_seconds",
                    "numpy_version", "counts_sha256")},
                  f, indent=1)
    print(json.dumps(summary, indent=1))
    print(f"gen_on_dbx: wrote fact/fact2/dim parquet to {args.outdir} in {gen_s:.1f}s")


if __name__ == "__main__":
    main()
