# E3 — Near-Threshold Depth Sensitivity

Executed 2026-11-25 under `FINAL_EXECUTION_PROTOCOL.md` §3 (plus §2 byte
conventions and §7 statistics). Environment for all predictions: local,
system python3 3.12.3 / numpy 1.26.4, cwd `code/` (the environment of the
Nov 24 study runs). All study code used unmodified (`arms`,
`ablation.truncate_syn`, `classical_arms.predict_A9`, `common`).
Classification of every number below: **simulated** (count-level analytic
predictions; no executed workload was run). The independent experimental
unit is the dataset (n = 8 main grid; n = 4 extension); instance-level
figures are descriptive over correlated instances (§7).

Driver: `final_scientific_strengthening/code/e3_driver.py`.
Per-instance outputs: `results/predictions_e3_new.csv` (new arm-points),
`results/predictions_e3_hash_sensitivity.csv`,
`results/predictions_e3_extension.csv`; machine-readable summaries:
`results/e3_summary.json`, `results/e3_extension_summary.json`,
`results/e3_consistency_check.json`, `results/rows_e3.csv`.

## 1. Consistency check (gate — PASSED)

GT loads/verdicts recomputed from each dataset's `counts.npz` via
`common.loads_from_counts` (exact distinct counts) + `common.verdict`,
and compared against the existing `results/exp_a/predictions_near_*.csv`:

- **88/88 instances: gt_verdict matches exactly; 0 mismatches.**
- Max |recomputed GT load − recorded GT load| = **0.0** (exact, not just
  to the CSVs' 0.1 rounding).
- GT composition confirmed: **62 GT FAIL / 26 GT PASS** of 88.

Environment cross-check (not a protocol gate, reported for the record):
fresh A8@1024 and A9@1024 predictions computed in this environment differ
from the recorded CSV pred_loads by at most **0.050** (A8) / **0.045**
(A9) — exactly the CSVs' 0.1 rounding. The environment reproduces the
existing rows.

## 2. Main grid results (88 instances; recall denominator 62, specificity denominator 26)

Existing rows reused unchanged: A1, A5, A7, A8@1024, A9@1024. New runs:
A8 at k ∈ {16, 64, 256} (deterministic; the seed argument is immaterial
for A8's r=None path, as frozen) and A9 at targets {16, 64, 256}.

| Point | TP | FP | FN | TN | Recall (/62) | Specificity (/26) | Median Q | Worst-case Q | Bytes mean (min–max) |
|---|---|---|---|---|---|---|---|---|---|
| A1 | 38 | 0 | 24 | 26 | 0.613 | 1.000 | 1.020 | 39.89 | — (testbed arm) |
| A5 | 62 | 0 | 0 | 26 | 1.000 | 1.000 | 1.000 | 2.740 | — (testbed arm) |
| A7 | 61 | 0 | 1 | 26 | 0.984 | 1.000 | 1.013 | 9.666 | — (packet arm, existing) |
| A8@16 | 54 | 0 | 8 | 26 | 0.871 | 1.000 | 1.001 | 1.188 | 2,153 (2,147–2,158) |
| A8@64 | 56 | 0 | 6 | 26 | 0.903 | 1.000 | 1.001 | 1.085 | 6,463 (6,377–6,548) |
| A8@256 | 56 | 0 | 6 | 26 | 0.903 | 1.000 | 1.001 | 1.106 | 23,648 (23,565–23,732) |
| A8@1024 | 60 | 0 | 2 | 26 | 0.968 | 1.000 | 1.000 | 1.028 | 91,876 (91,694–92,039) |
| A9@16 | 62 | 26 | 0 | 0 | 1.000 | **0.000** | 3.389 | 1070.2 | 1,791 (1,740–1,872) |
| A9@64 | 62 | 0 | 0 | 26 | 1.000 | 1.000 | 1.173 | 267.6 | 4,630 (4,580–4,663) |
| A9@256 | 62 | 0 | 0 | 26 | 1.000 | 1.000 | 1.012 | 66.89 | 14,506 (14,430–14,557) |
| A9@1024 | 62 | 0 | 0 | 26 | 1.000 | 1.000 | 1.002 | 16.72 | 60,434 (60,254–60,878) |

Bytes conventions (§2): A8 = `len(json.dumps(truncated synopsis))` with
default separators; A9 = compact-separator packet JSON size. The two
conventions differ and are not merged. A8@1024 bytes are the existing
`synopsis_bytes`; A9@1024 bytes are the existing on-disk packet sizes.

### Reading of the table (negatives stated plainly)

- **A8 loses recall monotonically-ish as depth shrinks**: 0.968 @1024 →
  0.903 @64 and @256 → 0.871 @16. All A8 errors at every depth are false
  negatives on join pipelines (J_A/J_B/J_N); specificity is 1.000 at
  every depth (0 FP in 26). The @256 point does not beat @64 on recall
  (identical FN set) and is slightly worse on worst-case Q (1.106 vs
  1.085) — depth response on this grid is **not** strictly monotone.
- **A9@16 collapses on specificity: 0.000 (0/26).** At target 16 the
  end-biased threshold T is so large that every retained tail value is
  Horvitz–Thompson-inflated to T, pushing all 26 GT-PASS instances over
  the FAIL threshold (FPs: G_A ×6, G_B ×6, G_C ×8, W_A ×6). Recall stays
  1.000 only trivially (everything is predicted FAIL). A9@16 is not a
  usable operating point on this grid.
- **A9 verdicts are perfect at targets 64/256/1024 (62/62 recall, 26/26
  specificity), but load fidelity at low targets is poor where verdicts
  do not bind**: worst-case Q 267.6 @64 and 66.9 @256, concentrated in
  G_C (the uniform column: a retained tail value is reconstructed as T,
  orders of magnitude above its true count ~500, yet still below the
  200,000 load threshold, so no verdict flips). Even @1024 the
  worst-case Q is 16.72 (again G_C). Verdict fidelity and load fidelity
  diverge sharply for A9 on this grid; neither should be reported
  without the other.
- A5 is perfect on verdicts here (62/62, 26/26) but carries the grid's
  A8-family worst load error short of A9's (max Q 2.740, G family).
  A1's 24 FNs are all join pipelines (J family median Q 17.7).

### FN/FP by pipeline, with per-error GT-load/threshold ratio

A8 (all errors are FN; ratio = GT load / threshold, so 1.02 = 2% over):

- A8@16 FN (8): near_s650_s101 J_A 1.065, J_B 1.068, J_N 1.024;
  near_s650_s202 J_A 1.064, J_B 1.067, J_N 1.023;
  near_s680_s101 J_N 1.156; near_s680_s202 J_N 1.158.
- A8@64 FN (6) and A8@256 FN (6): the same six s650 instances above
  (J_A/J_B/J_N on both s650 seeds).
- A8@1024 FN (2): near_s650_s101 J_N 1.024; near_s650_s202 J_N 1.023.
- A8 FP: none at any depth (0/26 at k = 16/64/256/1024).

Every A8 miss is a genuinely near-threshold instance (GT/threshold ≤
1.16); there are no large-margin misses at any depth.

A9: no FN at any target. A9@16 FP (26, all GT-PASS instances; ratio =
GT load / threshold, all < 1 as required for GT PASS): G_A/G_B/W_A on
all 8 datasets (ratios 0.559–0.926, rising with s) and G_C on all 8
datasets (ratio 0.003 — the uniform column sits at ~0.3% of threshold
in truth and is inflated past it purely by the HT reconstruction).
A9@64/@256/@1024: no errors of either kind.

Reused arms: A1 FN = 24, all J (J_A ×8, J_B ×8, J_N ×8 — every join
instance on the grid); A7 FN = 1 (J_N); A5: no errors.

### Per-family Q-error (median / max), per point

| Point | G (n=32) | J (n=24) | F (n=16) | W (n=8) | D (n=8) |
|---|---|---|---|---|---|
| A1 | 1.013 / 2.740 | 17.736 / 39.888 | 1.026 / 1.107 | 1.013 / 1.018 | 1.006 / 1.009 |
| A5 | 1.000 / 2.740 | 1.017 / 1.050 | 1.000 / 1.001 | 1.000 / 1.000 | 1.001 / 1.003 |
| A7 | 1.019 / 9.666 | 1.020 / 1.058 | 1.017 / 1.047 | 1.009 / 1.013 | 1.004 / 1.008 |
| A8@16 | 1.000 / 1.000 | 1.169 / 1.188 | 1.013 / 1.019 | 1.000 / 1.000 | 1.001 / 1.003 |
| A8@64 | 1.000 / 1.000 | 1.070 / 1.085 | 1.002 / 1.003 | 1.000 / 1.000 | 1.001 / 1.003 |
| A8@256 | 1.000 / 1.000 | 1.088 / 1.106 | 1.002 / 1.004 | 1.000 / 1.000 | 1.001 / 1.003 |
| A8@1024 | 1.000 / 1.000 | 1.021 / 1.028 | 1.000 / 1.000 | 1.000 / 1.000 | 1.001 / 1.003 |
| A9@16 | 3.937 / 1070.2 | 4.348 / 5.875 | 1.264 / 2.920 | 3.939 / 5.592 | 1.257 / 1.482 |
| A9@64 | 1.060 / 267.6 | 1.785 / 2.203 | 1.067 / 1.373 | 1.061 / 1.398 | 1.026 / 1.173 |
| A9@256 | 1.000 / 66.89 | 1.266 / 1.373 | 1.016 / 1.032 | 1.000 / 1.000 | 1.001 / 1.003 |
| A9@1024 | 1.000 / 16.72 | 1.058 / 1.067 | 1.002 / 1.005 | 1.000 / 1.000 | 1.001 / 1.003 |

A8's residual error at low depth is entirely a J-family phenomenon
(median Q 1.169 @16 → 1.021 @1024); its G/W medians are exactly 1.000 at
every depth (the top key is always stored). A9's J median Q is worse
than A8's at matched-ish budgets (1.785 @64 vs 1.070 @64).

## 3. F_B investigation (near grid)

F_B Q-error (n = 8 per point; F_B is GT FAIL on all 8 near datasets, so
this is load fidelity only — no verdict ever flips on F_B here):

| Point | F_B median Q | F_B max Q |
|---|---|---|
| A1 | 1.057 | 1.107 |
| A5 | 1.000 | 1.000 |
| A7 | 1.017 | 1.041 |
| A8@16 | 1.013 | 1.019 |
| A8@64 | 1.002 | 1.003 |
| A8@256 | 1.002 | 1.004 |
| A8@1024 | 1.000 | 1.001 |
| A9@16 | 1.267 | 2.920 |
| A9@64 | 1.067 | 1.373 |
| A9@256 | 1.016 | 1.032 |
| A9@1024 | 1.002 | 1.005 |

On the near grid, F_B is a non-issue for A8 at every depth (max Q ≤
1.019 even at k=16) and modest for A9 (worst 2.920 at target 16,
improving with target). **Caveat, stated plainly per the frozen
protocol: the near grid is Zipf-only, so the headline *mixed-family*
F_B underprediction (worst-case Q 6.41 @ k=16 / 3.17 @ k=64 on the
headline grid) cannot be re-tested here and is instead characterised
on the headline grid in E2.** Nothing in this section weakens or
confirms that headline F_B finding; the near grid's F_B behaviour is a
different (single-family Zipf) regime, where the aligned-pair synopsis
and the Eq. 1 estimator both track F_B closely.

## 4. J_B hash sensitivity (A9, near grid)

A9 at targets {1024, 64} under the two sensitivity hashes
(`HASH_SEEDS_SENSITIVITY` = 0x9E3779B97F4A7C15 ["hash1"],
0x6A09E667F3BCC909 ["hash2"]), flips counted against the headline hash
H0 at the same target. **The sensitivity hashes are correlated by
construction (h_{H+1}(v) = h_H(v+1)) and are not independent
replications** — stated here as in the Exp A preregistration.

| Hash / target | Flips overall (/88) | J_B flips (/8) | Recall (/62) | Specificity (/26) |
|---|---|---|---|---|
| hash1 @1024 | **7** | **2** | 0.887 (55 TP, 7 FN) | 1.000 |
| hash1 @64 | 0 | 0 | 1.000 | 1.000 |
| hash2 @1024 | 0 | 0 | 1.000 | 1.000 |
| hash2 @64 | 0 | 0 | 1.000 | 1.000 |

The hash1 @1024 flips are all FAIL→PASS (false negatives) on join
pipelines of the lowest-s datasets: near_s650_s101 J_A, J_B, J_N;
near_s650_s202 J_B, J_N; near_s680_s101 J_N; near_s680_s202 J_N — of
which the J_B flips are near_s650_s101 and near_s650_s202. So A9's
perfect headline-hash verdict record at target 1024 is **not**
hash-robust on this grid: one of the two sensitivity hashes drops
recall to 0.887, with J_B specifically flipping on 2 of 8 datasets.
At target 64 neither sensitivity hash flips any verdict (the coarser
operating point's verdicts sit further from the threshold in hash
space). This is a genuine negative for A9@1024 and is reported as such;
with only two correlated sensitivity hashes, no flip *rate* is claimed.

## 5. Held-out extension grid (reported separately)

4 NEW datasets generated with the unmodified `gen_near_threshold.generate`
into `final_scientific_strengthening/data_ext/` (never into `data/`):
s ∈ {0.70, 0.78} × seeds {404, 505}. Generation environment: the
`realdata_arm/venv` interpreter (python 3.12.3, **numpy 2.5.5**, duckdb
2.0.1) — different from the study environment (numpy 1.26.4); this
affects only the generation of these new datasets, and all predictions
on them were run in the study environment. **Validation: all 4 datasets
`validation_ok = true` with every DuckDB check exactly 0 (all-exact),
as required.** GT composition: 34 GT FAIL / 10 GT PASS of 44
(s700 datasets 7 FAIL each; s780 datasets 10 FAIL each). This is a
held-out extension of the **same generator family** — independent
instances, not an independent generating process — and the near grid
itself is a follow-up benchmark, never an independent external holdout.

| Point | TP | FP | FN | TN | Recall (/34) | Specificity (/10) | Median Q | Worst-case Q |
|---|---|---|---|---|---|---|---|---|
| A1 | 22 | 0 | 12 | 10 | 0.647 | 1.000 | 1.044 | 50.60 |
| A5 | 34 | 0 | 0 | 10 | 1.000 | 1.000 | 1.000 | 3.231 |
| A8@1024 | 34 | 0 | 0 | 10 | 1.000 | 1.000 | 1.000 | 1.023 |
| A8@64 | 34 | 0 | 0 | 10 | 1.000 | 1.000 | 1.000 | 1.072 |
| A9@1024 | 34 | 0 | 0 | 10 | 1.000 | 1.000 | 1.001 | 16.61 |
| A9@64 | 34 | 0 | 0 | 10 | 1.000 | 1.000 | 1.015 | 265.7 |

On the extension grid every synopsis/classical point in the frozen arm
set is verdict-perfect, **including A8@64 (34/34)** — unlike the main
grid, where A8@64 misses 6. The difference is the instance mix: the
extension's s ∈ {0.70, 0.78} datasets put fewer join instances within a
few percent of the threshold (the main grid's misses all sit at
GT/threshold ≤ 1.16, concentrated at s = 0.65). A1 reproduces its main-grid
failure mode exactly (12 FN, all J; J median Q 36.7). A9's G_C load
fidelity problem also reproduces (worst-case Q 265.7 @64, 16.61 @1024,
verdicts unaffected). Bytes on the extension grid: A8@1024 mean 91,981;
A8@64 mean 6,529; A9@1024 mean 60,070; A9@64 mean 4,637 (same conventions
as §2).

## 6. What E3 does and does not show

- Depth response, near grid: A8 verdict recall degrades as k shrinks
  (0.968 → 0.871) with all misses near-threshold J instances; A9 verdicts
  are perfect at targets ≥ 64 but its load fidelity (esp. G_C) degrades
  steeply as target shrinks, and target 16 is degenerate (specificity 0).
- A9@1024's verdict perfection is hash-sensitive (hash1: recall 0.887,
  J_B flips 2/8); A9@64's is not, on the two correlated hashes tested.
- F_B is benign on this Zipf-only grid at all depths for both arms; the
  headline mixed-family F_B weakness is untouched by this experiment
  and lives in E2.
- The extension grid (44 instances, same generator family) corroborates
  the qualitative pattern (A1 join blindness; A9 G_C load inflation;
  A8/A9 verdict strength at s ≥ 0.70) but does not reproduce the main
  grid's A8 low-depth misses, because its instances sit further from
  the threshold.
- Nothing here is an executed-workload result: all figures are
  count-level simulated predictions, and no threshold violation is an
  observed execution failure.
