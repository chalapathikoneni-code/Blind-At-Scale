# Experiment A — results (end-biased sampling baseline A9 + near-threshold)

Pre-registration: `~/workspace/blind-at-scale/PREREGISTRATION_EXP_A.md`
(locked 2026-11-24 01:30 EST, pre-run; Amendment 1, 01:33 EST, pre-run:
sensitivity hash seeds). Report: `~/workspace/blind-at-scale/EXP_A_REPORT.md`.
Run date: 2026-11-24. Local only, no spend, no cloud, no Drive uploads.

## Environment

- Python: `~/workspace/ieee-access-stress-test/venv/bin/python`
  (Python 3.12.3, numpy 2.5.4, scipy 1.18.1, duckdb 2.0.0, pandas 3.1.1)
  — the study's pinned environment (reproduction_package_staging/
  environment_requirements.txt). System python3 has numpy 1.26.4 and
  no duckdb; it was NOT used for any run.
- A9 has no numpy RNG stream (all sampling decisions come from the
  fixed hash), so its results are immune to the numpy-version caveat.
  A1/A5/A7 on the near grid use the study venv (numpy 2.5.4), same as
  the original program.

## Reproduction gate (passed, before any A9 run)

Re-ran A1-A8 on a COPY of zipf_s101 counts in /tmp (never overwriting
data/zipf_s101/predictions_main.csv) with the full arm list (so A5's
seed matches the main-grid convention):
`python code/run_dataset.py --dataset-dir /tmp/repro_zipf_s101
--arms A1,A2,A3,A4,A5,A8 --rtag repro`.
Result: all 66 rows match predictions_main.csv exactly on gt_load,
gt_verdict, pred_load, pred_verdict, testbed_fact_rows, r (the only
differences are the `dataset` column, which is the directory basename);
rebuilt synopsis.json is byte-identical (sha256 6e780208eec165a0...).
A5 zipf_s101 median Q = 1.000117 (pooled over 99: 1.001465, recorded).

## Implementation validation (before any experiment run)

- Threshold search: zipf s=1 column gives T=5323, E(T)=1023.9 <= 1024,
  E(T-1)=1024.0 > 1024; uniform column gives T=9766 > max count 595.
- Eq. 1 unbiasedness: Monte Carlo over 500 well-separated hash seeds
  on a synthetic pair gave mean estimated/true join size 1.0000
  (SE 0.0017); per-value HT checks consistent with unbiasedness;
  exact-head values exact in every draw; packet deterministic given
  counts + hash seed.
- Adjacent hash seeds are correlated by the shift property
  h_{H+1}(v) = h_H(v+1) (verified in code) — hence Amendment 1
  (well-separated sensitivity seeds).

## Command lines (cwd = ~/workspace/blind-at-scale/code)

    python run_exp_a.py            # A9 on the 99 + 40M (H0, H1, H2)
    python run_near_threshold.py   # generate 8 near datasets + run arms
    python score_exp_a.py          # all scoring + decision rule

Seeds: A9 headline hash H0 = 0; sensitivity H1 = 0x9E3779B97F4A7C15,
H2 = 0x6A09E667F3BCC909 (Amendment 1). Near grid: A1/A5/A8 via
arms.run_arms (full arm list; A5 seed = dataset_seed*1000 + 17*4 + 5,
the main-grid convention); A7 seed = dataset_seed*1000 + 5000
(Amendment 5 convention). Bootstrap seed 7 (score.py convention).

## File inventory (72 files, 1.6 MB)

- `predictions_exp_a_<tag>.csv` (10): A9 headline (H0) predictions,
  standard schema, for the 9 headline datasets + zipf_s101_40m.
  A9 rows carry testbed_fact_rows = 0 and an empty `r` (no
  row-sampling rate applies to a direct-prediction arm).
- `predictions_exp_a_<tag>_h1.csv`, `predictions_exp_a_<tag>_h2.csv`
  (20): A9 sensitivity-hash predictions, same schema.
- `predictions_near_<tag>.csv` (8): near-threshold predictions for
  arms A1/A2/A3/A4/A5/A7/A8/A9, standard schema.
- `packet_A9_<tag>.json` (18): serialised end-biased samples (compact
  JSON) for the 10 headline/scale + 8 near datasets.
- `packet_A7_<tag>.json` (8): A7 packets for the near datasets.
- `summary_exp_a.json`: pooled metrics, splits, McNemar, bootstrap,
  packet bytes, hash sensitivity, 40M scale check, decision-rule
  evaluation (Branch 1 — NARROW).
- `summary_near.json`: near-threshold pooled/per-s/per-pipeline
  metrics including false-positive rates.
- `threshold_sensitivity.json`: verdict flips at T*0.8 / T*1.2
  (re-derived from recorded loads, no re-run), both grids.
- `metrics_exp_a.csv`: pooled 99-instance metrics table (A1-A9).
- `run_info.json`: per-dataset thresholds, retained counts, packet
  bytes, GT cross-check results (0 mismatches, max GT diff 0.0).
- `run_exp_a.log`, `run_near_threshold.log`: driver logs.
- `README.md` (this file).

Near-threshold datasets (new dirs, DuckDB-validated, all_exact):
data/near_s650_s101, near_s650_s202, near_s680_s101, near_s680_s202,
near_s720_s101, near_s720_s202, near_s740_s101, near_s740_s202
(~215 MB each incl. parquet; synopsis.json built by unmodified
arms.build_synopsis).

## Deviations from the pre-registration (recorded here, per its rules)

1. Amendment 1 (pre-run, in PREREGISTRATION_EXP_A.md): sensitivity
   hash seeds changed from {1, 2} to well-separated constants (shift
   property). Headline H0 = 0 unchanged. No experiment run had
   occurred.
2. Pre-registered expectation violated (measured outcome, not a
   design change): J_A/J_B were expected to straddle T at s = 0.65
   (analytic hottest-partition estimate ~190k, 0.95xT) but realised
   J_A = 212.8-213.0k (FAIL) at s = 0.65 on both seeds — the analytic
   estimate understated the realised max partition. J pipelines are
   FAIL at all four s values; the straddle lives on G_A/G_B/W_A
   (PASS at s <= 0.72, FAIL at 0.74) exactly as designed, and J_N
   provides the closest-to-threshold FAILs (204.6k, 1.023xT).
3. Pre-registered expectation violated: no false positives appeared
   on the near grid (FPR = 0.0 for every arm). All near-threshold
   errors are false negatives (A7 1, A8 2, A4 2, A1 24, A2 35, A3 45).
4. Pre-registered expectation violated: A9's pooled median Q-error
   (1.00155) is far better than the expected 1.0-1.1 range and
   essentially ties A5 (1.00147); it beats A7 (1.01534). The
   G_C weakness predicted in §2.4 did materialise (median Q 16.55)
   but does not move the pooled median.
5. No deviations in thresholds, decision rule, arm definitions, or
   scoring: all as pre-registered. 100M not run, as pre-registered
   (§3.2: no local 100M fact counts exist).

## Headline numbers

- A9 (99): recall 1.000, precision 1.000, Spearman rho 0.9966,
  median Q-error 1.00155; packet 58.6-64.4 KB vs synopsis 91.4-100.3
  KB (ratio 0.639-0.648). Decision rule: Branch 1 — NARROW.
- A9 (40M): recall 1.000 (8/8), rho 1.0000, median Q 1.00003.
- A9 (near 88): recall 1.000 (62/62), FPR 0.0, rho 0.9987,
  median Q 1.00207. A5 also 62/62; A7 61/62; A8 60/62; A1 38/62.
- Hash sensitivity: H1 0/110 verdict disagreements vs H0; H2 4/110
  (all zipf J_B false positives — see EXP_A_REPORT.md §6).
