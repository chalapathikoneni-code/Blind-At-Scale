# Item 5 — Equal-information sampling arms A7 / A7X

Study: PREREGISTRATION.md **Amendment 5 §A5.1** (locked 2026-11-06
12:15 UTC, pre-run; hostile re-review R2 item 5). Purpose: separate
"the synopsis as a method" from "any consumer of the same
heavy-hitter information". All numbers below are taken from
`results/eqinfo/summary.json` (produced by `code/score_eqinfo.py`
from `data/<tag>/predictions_eqinfo.csv`); reference-arm numbers are
the values copied into that file from `results/summary.json`.
Run date: 2026-11-06. Local only, no spend.

## Design recap (Amendment 5 §A5.1, as executed)

Both arms are row samplers implemented at count level (Amendment 1c:
Bernoulli row sampling of a column with per-key counts c_k yields
Binomial(c_k, r) sampled counts). Both run on the existing 9 headline
datasets (99 instances); ground truth and A1–A5/A8 were NOT re-run —
ground truth was recomputed from `counts.npz` via
`common.loads_from_counts` with `distinct_counts=counts['distinct_A']`
(as `arms.run_arms` does), A5 comparisons read the recorded
`predictions_main.csv` verdicts, and A1–A5/A8 comparison values read
`results/summary.json`. Loads use `common.loads_from_counts` with
`dim_present` = all True and D_A by the Amendment 1a occupancy
estimator; verdicts via `common.verdict`. Scoring mirrors §6 /
`score.py`: pooled recall/precision/F1 (FAIL positive), Spearman rho
on log10 loads, median Q-error = median(max(p/g, g/p)).

- **A7 (HH-stratified sampler, ids-only packet).** Packet per dataset:
  per fact column (kA, kB, kC, kN) and per fact2 column (kA, kB), the
  top-1024 heavy-hitter key ids (**no counts**), the column total and
  the column tail mass; plus `dim_rows` = K. Fact HH ids come from the
  profiler synopsis (`syn['cols'][col]['hh_keys']`); fact2 HH ids are
  the top-1024 by fact2 counts computed from `counts.npz`. Per column,
  two strata (hot = keys in that column's HH list; tail = rest) with
  expected sampled rows r × total (r = 0.01) allocated beta = 0.9 to
  hot: r_hot = 0.9·0.01/f_hot, r_tail = 0.1·0.01/(1 − f_hot),
  f_hot = (total − tail_mass)/total. Sampled counts
  Binomial(c_k, r_stratum(k)); Horvitz–Thompson prediction
  sampled/r_stratum. kN NULL rows are tail:
  Binomial(null_count, r_tail(kN))/r_tail(kN). F pairs use each
  table's own stratified estimate (own HH list, independent draws),
  combined by the common product form.
- **A7X (exact-HH sampler, counts packet).** Packet as A7 plus exact
  HH counts per column (`hh_counts`; fact2's own counts at its own
  top-1024 keys). HH counts are taken as known. All columns' tails are
  sampled at a single per-dataset rate
  r_tail = 0.01 × N_fact / tail_mass_fact(kA), with
  N_fact = `counts['fact_kA'].sum()`. Predicted counts: exact on HH
  keys, sampled/r_tail on tail keys. kN NULL rows:
  Binomial(null_count, r_tail)/r_tail.
- **Packets** are serialised compactly (`separators=(',',':')`) to
  `packet_A7_<tag>.json` / `packet_A7X_<tag>.json` in this directory
  and byte-costed against the dataset's `synopsis.json`.
- **Testbed size** follows the existing convention: sampled fact rows
  counted on kA plus sampled NULL rows (as `arms.py` A1). For A7X the
  kA count is the sampled tail rows on kA (HH rows are known, not
  sampled) and NULL rows are sampled at the single r_tail, so A7X
  sizes sit above the A7 sizes.
- **RNG:** seed = dataset_seed × 1000 + 5000 + arm_index (A7: 0,
  A7X: 1), `numpy.random.default_rng`. Binomial draws use int64
  counts, as in `arms.py`.

## Packet byte sizes (from summary.json `packet_bytes`)

| dataset | synopsis B | A7 packet B | A7 / synopsis | A7X packet B | A7X / synopsis |
|---|---|---|---|---|---|
| zipf_s101 | 91379 | 26002 | 0.2846 | 55035 | 0.6023 |
| zipf_s202 | 91398 | 26026 | 0.2848 | 55056 | 0.6024 |
| zipf_s303 | 91351 | 25999 | 0.2846 | 55015 | 0.6022 |
| lognormal_s101 | 100290 | 33746 | 0.3365 | 63396 | 0.6321 |
| lognormal_s202 | 100308 | 33781 | 0.3368 | 63423 | 0.6323 |
| lognormal_s303 | 100300 | 33764 | 0.3366 | 63405 | 0.6322 |
| mixed_s101 | 91681 | 29618 | 0.3231 | 56958 | 0.6213 |
| mixed_s202 | 91732 | 29651 | 0.3232 | 56991 | 0.6213 |
| mixed_s303 | 91703 | 29624 | 0.3230 | 56964 | 0.6212 |

Ranges: A7 packets 25999–33781 B; A7X packets 55015–63423 B;
synopses 91351–100308 B. Maximum packet/synopsis ratio: A7 0.3368,
A7X 0.6323. **Neither packet exceeds its dataset's synopsis size on
any dataset** (`packet_exceeds_synopsis_any`: A7 false, A7X false),
matching the pre-registered expectation that both packets are
smaller than the synopsis.

Mean testbed fact rows: A7 101511.6, A7X 114367.4 (per-dataset values
in summary.json `testbed_rows_per_dataset`).

## Headline results — pooled over 99 instances

| arm | recall | precision | F1 | Spearman rho | median Q-error | TP/FP/FN/TN |
|---|---|---|---|---|---|---|
| A1 | 0.7667 | 1.0000 | 0.8679 | 0.8090 | 1.0435 | 46/0/14/39 |
| A2 | 0.5167 | 1.0000 | 0.6813 | 0.8697 | 2.1658 | 31/0/29/39 |
| A3 | 0.4167 | 1.0000 | 0.5882 | 0.7944 | 2.9060 | 25/0/35/39 |
| A4 | 1.0000 | 1.0000 | 1.0000 | 0.9948 | 1.0196 | 60/0/0/39 |
| A5 | 1.0000 | 1.0000 | 1.0000 | 0.9986 | 1.0015 | 60/0/0/39 |
| A8 | 1.0000 | 1.0000 | 1.0000 | 0.9996 | 1.0000 | 60/0/0/39 |
| **A7** | **1.0000** | **1.0000** | **1.0000** | **0.9960** | **1.0153** | **60/0/0/39** |
| **A7X** | **1.0000** | **1.0000** | **1.0000** | **0.9992** | **1.0005** | **60/0/0/39** |

Exact values: A7 rho 0.995989, median Q 1.015336; A7X rho 0.999224,
median Q 1.000487; A5 median Q 1.001465 (summary.json).

Per-pipeline-family splits (recall/precision/F1; summary.json
`per_pipeline_family`):

| split | A7 | A7X |
|---|---|---|
| direct (G,D), n=45 | 1.0000/1.0000/1.0000 | 1.0000/1.0000/1.0000 |
| join-emergent (J,F), n=45 | 1.0000/1.0000/1.0000 | 1.0000/1.0000/1.0000 |
| window (W), n=9 | 1.0000/1.0000/1.0000 | 1.0000/1.0000/1.0000 |

Join-emergent load fidelity: A7 rho 0.994598, median Q 1.015336;
A7X rho 0.998946, median Q 1.001059. Median Q-error by pipeline
group: F family (n=18) A7 1.016847, A7X 1.000024; J family (n=27)
A7 1.012929, A7X 1.001794. Per-distribution-family splits are in
summary.json `per_distribution_family` (recall 1.0000 for both arms
in all three families).

## Pre-registered comparisons vs A5

- **McNemar exact test** (verdict correctness, 99 instances):
  A5 vs A7 — only-A5-correct 0, only-A7-correct 0, p_exact 1.0.
  A5 vs A7X — only-A5-correct 0, only-A7X-correct 0, p_exact 1.0.
  (No discordant instances: A5, A7 and A7X are all correct on all
  99 instances.)
- **Paired bootstrap of delta-recall** (10,000 resamples over the
  99 instances, `default_rng(7)`, percentile 95% CI):
  A5 − A7 point 0.0, bootstrap mean 0.0, CI95 [0.0, 0.0].
  A5 − A7X point 0.0, bootstrap mean 0.0, CI95 [0.0, 0.0].

## Interpretation rule and outcome

Pre-registered rule (Amendment 5 §A5.1): *if A7 matches A5 on recall
AND its median Q-error is within 10% relative of A5's, the
manuscript's mechanism claim narrows to "any consumer of the
heavy-hitter list"; if only A7X matches on both, the claim narrows
to "stored exact heavy-hitter counts, consumed by sampling or
prediction alike"; if neither matches A5 on load fidelity, the
method-vs-information separation stands as written.*

Evaluation (summary.json `interpretation`): A7 recall 1.0000 =
A5 recall 1.0000, and A7 median Q-error 1.015336 differs from A5's
1.001465 by 1.385% relative — within 10%. A7X also matches on both
(recall 1.0000; median Q relative difference 0.098%), but the rule
is evaluated on A7 first.

**Branch 1 fired: A7 matches A5 on both criteria, so the mechanism
claim narrows to "any consumer of the heavy-hitter list".**

## Sanity checks

- Ground-truth cross-check: recomputed GT verdicts match the
  `gt_verdict` column of `predictions_main.csv` on all 99 instances
  (0 mismatches; max abs GT load difference 0.0).
- A1 reproduction: `score_eqinfo.py`'s scoring path applied to the
  A1 rows of `predictions_main.csv` reproduces A1's pooled numbers
  in `results/summary.json` exactly (recall 0.7667, precision
  1.0000, F1 0.8679, rho 0.8090, median Q 1.0435, TP/FP/FN/TN
  46/0/14/39; `matches: true`).

## Deviations

None. No arm definition, packet content, seed, threshold, or
interpretation rule was changed after the run; no existing file was
modified. New files: `code/eqinfo_arms.py`, `code/run_eqinfo.py`,
`code/score_eqinfo.py`, `data/<tag>/predictions_eqinfo.csv` (9),
`results/eqinfo/packet_A7_<tag>.json` and `packet_A7X_<tag>.json`
(18), `results/eqinfo/summary.json`, `results/eqinfo/metrics.csv`,
and this README.
