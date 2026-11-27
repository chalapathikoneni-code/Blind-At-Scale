# E5 — Profiling and Deployment Overhead (Blind at Scale, Final Scientific Strengthening)

Executed 2026-11-25 under `FINAL_EXECUTION_PROTOCOL.md` §5. All numbers
below are **measured (local timing)**; medians of 3 repetitions, each
repetition in a fresh subprocess. Raw per-repetition results:
`results/e5_raw.json`; machine-readable medians: `results/rows_e5.csv`.
Code: `code/e5_driver.py`, `code/e5_phase.py`, `code/e5_summarize.py`
(new files only; no existing file or manuscript was modified).

## Conditions

- **Hardware:** AMD EPYC 9D25, 4 visible CPUs, shared VM.
- **Software:** Python 3.12.3, numpy 2.5.5, pyarrow 26.0.1, DuckDB
  2.0.1 — the `realdata_arm/venv` interpreter, as frozen in §5 (raw
  parquet scanning requires pyarrow/duckdb). The numpy version differs
  from the study runs (numpy 1.26.4); E5 values are timings only and
  no E5 value enters any fidelity claim.
- **Datasets (one family per scale, first available tag, seed 101 for
  all):** `outeq_200k_zipf_s101` (200K fact rows), `outeq_1m_zipf_s101`
  (1M), `zipf_s101` (10M), `zipf_s101_40m` (40M).
- **Cache caveat:** each repetition ran in a fresh process; OS page
  cache state was **not controlled** (later repetitions may benefit
  from a warm page cache).
- **Contention caveat:** other campaign jobs were running on this VM
  concurrently (load average ≈ 3.6 on 4 CPUs at start). Times are
  therefore upper-bound-ish and noisier than on an idle machine;
  medians of 3 are reported for that reason.
- Prediction phases operate on counts-level arrays (key space
  K = 20,000 at every scale), so prediction times are expected to be
  roughly scale-independent; the scan phase is the only phase whose
  cost grows with the raw data volume.

## Counts validation (gate for §5b)

The DuckDB scan counts were compared against each dataset's
`counts.npz` over **all** arrays (`fact_kA/kB/kC/kN`, `fact2_kA/kB`,
`distinct_A`, `null_count`): **max abs diff = 0 for every array, at
every scale, in all 3 repetitions** — including the required 10M
dataset (`zipf_s101`). Downstream phases (build / serialise /
predict) therefore consumed the scan-derived counts at all four
scales; no `counts.npz` fallback was needed.

## Measured

Median wall times (seconds) and peak-memory deltas (MB,
`resource.getrusage(RUSAGE_SELF).ru_maxrss` delta inside each phase's
subprocess):

| Scale (dataset) | Scan (a) | Build (b) | Serialise (c) | Predict A8 (d) | Predict A5 (d) | Predict A9 (d) | End-to-end (g) |
|---|---|---|---|---|---|---|---|
| 200K (`outeq_200k_zipf_s101`) | 1.279 | 0.013 | 0.0013 | 0.221 | 0.289 | 0.082 | 1.514 |
| 1M (`outeq_1m_zipf_s101`) | 2.121 | 0.014 | 0.0023 | 0.208 | 0.218 | 0.077 | 2.345 |
| 10M (`zipf_s101`) | 7.310 | 0.058 | 0.0014 | 0.320 | 0.232 | 0.013 | 7.690 |
| 40M (`zipf_s101_40m`) | 12.750 | 0.013 | 0.0015 | 0.205 | 0.215 | 0.012 | 12.969 |

| Scale | Rows scanned (fact + fact2) | Bytes scanned (fact + fact2 parquet) | Serialised synopsis (bytes) | Peak mem: scan | Peak mem: build | Peak mem: A8 / A5 / A9 |
|---|---|---|---|---|---|---|---|
| 200K | 200,000 + 120,000 | 3,876,249 + 636,448 | 74,887 | 98.6 MB | 1.2 MB | 11.4 / 12.0 / 3.0 MB |
| 1M | 1,000,000 + 600,000 | 18,588,544 + 3,181,312 | 81,412 | 125.2 MB | 1.2 MB | 11.3 / 11.9 / 3.0 MB |
| 10M | 10,000,000 + 6,000,000 | 185,630,276 + 31,621,527 | 91,379 | 266.0 MB | 1.5 MB | 11.4 / 11.9 / 3.1 MB |
| 40M | 40,000,000 + 24,000,000 | 742,124,238 + 126,487,955 | 96,514 | 725.1 MB | 1.5 MB | 11.5 / 11.8 / 3.1 MB |

Phase definitions, per §5:

- **(a) Profiler scan:** per-key counts for the fact table (kA, kB,
  kC, kN) and the fact2 table (kA, kB), plus `COUNT(DISTINCT s)` per
  kA and the kN null-sentinel count, from the raw parquet via DuckDB
  `GROUP BY` — the same counting queries the study generator used to
  produce `counts.npz`. Rows/bytes scanned are the fact + fact2 row
  counts and parquet file sizes (the scan reads both tables; the
  fact table alone is 200K/1M/10M/40M rows and
  3,876,249 / 18,588,544 / 185,630,276 / 742,124,238 bytes).
- **(b) Synopsis construction:** `arms.build_synopsis(counts)` from
  the validated scan-derived counts (heavy-hitter extraction
  included).
- **(c) Serialisation:** `json.dumps(synopsis)` with default
  separators (the study's on-disk synopsis format). Consistency
  check: at 10M the measured 91,379 B equals the on-disk
  `data/zipf_s101/synopsis.json` size exactly.
- **(d) Prediction:** `arms.arm_predictions` for A8 and A5
  (seed = dataset seed = 101) and `classical_arms.predict_A9(counts)`
  at its default target (1024 expected retained), per dataset.
- **(e) Testbed construction at counts level:** the A5 generation
  path runs inside `arm_predictions` and is included in the A5
  prediction time above. Materialising a testbed to disk is not part
  of the study machinery — see Not measured.
- **(f) Peak memory:** per-phase `ru_maxrss` deltas as tabulated;
  the largest is the 40M scan at **725.1 MB** — the observed peak
  across the 3 reps (725.1 / 715.5 / 692.5 MB; 715.5 MB is the
  median rep). Corrected 2026-11-26 per evidence-audit R8: earlier
  drafts quoted the median rep as the peak.
- **(g) End-to-end:** (a) + (b) + (c) + (d-A8), summed from the
  medians: 1.51 s (200K), 2.35 s (1M), 7.69 s (10M), 12.97 s (40M).
  The profiler scan dominates at every scale (84–98% of the total);
  everything after the scan is under 0.6 s combined at every scale.

Notes on the measured pattern: A9 prediction is *faster* at 10M/40M
(≈0.012 s) than at 200K/1M (≈0.08 s) in these medians; the phase is
counts-level (K = 20,000 keys at all scales), so no scale trend is
expected and the difference is attributed to the concurrent-load
contention stated above, not to scale. Synopsis build time is flat
(0.013–0.058 s) for the same reason: it operates on the 20,000-key
count vectors, not on rows.

## Not measured

- **Refresh cost — NOT MEASURED.** No incremental-refresh or
  re-profiling-on-update measurement was made, and **no
  refresh-efficiency claim** is supported by this experiment
  (per §5).
- **Deployment / cloud profiling cost — NOT MEASURED.** All timings
  are local to this shared VM. No cloud, EMR, or production
  deployment profiling cost was measured or estimated.
- **Testbed materialisation to disk — NOT MEASURED** (protocol §5e):
  writing a materialised A5 testbed to disk is not part of the study
  machinery; no materialisation number is reported and none was
  estimated. Only the counts-level A5 generation path (inside
  `arm_predictions`, timed in (d)) was measured.

Nothing in this experiment was estimated in place of a measurement.
