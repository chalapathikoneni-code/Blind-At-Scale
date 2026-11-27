# E7 — Real-Data Information Efficiency (FINAL_EXECUTION_PROTOCOL §6)

**Status: EXECUTED.** Instance set: NYC TLC **January 2026** only —
3,724,889 yellow + 40,272 green rows from the local parquet in
`realdata_arm/sample/`. **n = 1 dataset (one month); all figures are
descriptive only** (§7). Environment: local, realdata venv Python
3.12.3 (numpy 2.5.5, pyarrow 26.0.1). Classification: **analytical** —
the ground truth is the real-world model's analytic load and the
predictions are analytic; no execution was measured. Code:
`final_scientific_strengthening/code/e7_real_data.py`; machine-readable
rows: `final_scientific_strengthening/results/rows_e7.csv`;
full detail: `final_scientific_strengthening/results/e7_summary.json`.

## Consistency check (frozen gate) — PASS

Analytic ground-truth loads for 2026-01 were reproduced from the local
parquet with the packaged model (`realdata_model.exact_load`, counts
built exactly as in the Nov 23 dry run: NULL → `__NULL__`, else the
integer key as string; zone set from `taxi_zone_lookup.csv`). All 11
loads match `realdata_emr_package/config/analytic_gt_calibration.json`
for 2026-01 **exactly**, and all relative/absolute thresholds match to
1e-6:

G_A 199,975; G_B 191,226; G_C 10,596; G_N 2,390,495; J_A 199,975;
J_B 191,226; J_N 10,596; F_A 173,798,004; F_B 330,477,337;
W_A 199,975; D_A 259.

The frozen consistency rule was therefore not triggered. The estimator
was applied with its tail model unchanged (imported
`arms._tail_expected` / `arms._canonical_tail_order`; no
reimplementation) and loads were computed only with the model's own
load function (`realdata_model.partition_load` over contribution dicts
built per `contribution_map`'s definitions; `common.loads_from_counts`
was never used). The partner's RW-A8 was not substituted.

## Estimator application (adapter, no definition change)

Per column at each k ∈ {16, 64, 256, 1024}: exact top-k heavy-hitter
counts + power-law tail reconstruction. Key universe for the location
columns is the zone-lookup key set (LocationID 1..265) — the real-data
analog of the synthetic key space stored as `dim_rows`; tail keys are
the universe keys outside the HH set, in the estimator's canonical
(splitmix64) order. RatecodeID has no dimension table; its universe is
its 8 observed values, and its NULL group is carried at exact
`null_mass`, exactly as in the synthetic A8 path. F_A/F_B use the study
synopsis's pair convention: HH keys by the fact (yellow) column, both
tables' counts at those keys exact, tails fitted separately (table 2's
tail from table 2's own top-k counts) and assigned comonotonically.
D_A is diagnostic only (the model defines no prediction for distinct
workloads) and is excluded from every scored denominator below.

## Saturated groups — the instance set cannot discriminate verdicts

**All 10 scored pipelines are GT FAIL under both thresholds**
(primary relative 2× mean and secondary absolute 0.02×N): the scored
instance set is a single saturated all-FAIL group. (D_A is GT PASS,
259 < 50,000, but it is diagnostic and unscored.) Consequences:

- Verdict agreement below is identically recall; there are no GT-PASS
  instances, so precision and specificity are **not estimable** (false
  positives are impossible by construction in this set).
- Verdicts cannot discriminate estimator quality here at all; only
  load fidelity (Q-error) discriminates. Any claim from E7 is a
  load-fidelity claim on one all-FAIL month, nothing more.
- G_N is additionally exact by construction at every k: its
  cardinality is 8 ≤ 16, so even the smallest synopsis stores all its
  counts, and its NULL group is carried exactly.

## Results

Byte accounting: serialised truncated synopsis per k =
`len(json.dumps(synopsis_dict))` with default separators (§2 synopsis
convention); the synopsis contains the four standalone columns
(yellow PU/DO, green PU, yellow RatecodeID + null_mass) and the two
pair blocks (PU, DO).

| k | Synopsis bytes | Verdict agreement (rel / abs), of 10 | Recall (rel) | Median Q | Max Q | Worst underprediction |
|---|---|---|---|---|---|---|
| 16 | 2,247 | 10/10 / 10/10 | 1.000 (10/10) | 1.026 | 3.839 | 3.839 (F_A) |
| 64 | 6,240 | 10/10 / 10/10 | 1.000 (10/10) | 1.004 | 1.011 | 1.011 (G_B) |
| 256 | 19,999 | 10/10 / 10/10 | 1.000 (10/10) | 1.000 | 1.000 | 1.000 |
| 1024 | 20,211 | 10/10 / 10/10 | 1.000 (10/10) | 1.000 | 1.000 | 1.000 |

Per-pipeline Q-error (Q = max(GT/pred, pred/GT)):

| Pipeline | GT load | Q @ k=16 | Q @ k=64 | Q @ k=256 | Q @ k=1024 |
|---|---|---|---|---|---|
| G_A | 199,975 | 1.026 | 1.006 | 1.000 | 1.000 |
| G_B | 191,226 | 1.148 | 1.011 | 1.000 | 1.000 |
| G_C | 10,596 | 1.016 | 1.001 | 1.000 | 1.000 |
| G_N | 2,390,495 | 1.000 | 1.000 | 1.000 | 1.000 |
| J_A | 199,975 | 1.026 | 1.006 | 1.000 | 1.000 |
| J_B | 191,226 | 1.148 | 1.011 | 1.000 | 1.000 |
| J_N | 10,596 | 1.016 | 1.001 | 1.000 | 1.000 |
| F_A † | 173,798,004 | 3.839 | 1.002 | 1.000 | 1.000 |
| F_B † | 330,477,337 | 1.005 | 1.001 | 1.000 | 1.000 |
| W_A | 199,975 | 1.026 | 1.006 | 1.000 | 1.000 |

† **F_A/F_B are analytic-vs-analytic**: the ground truth itself is an
analytic product load, not a measured execution load. This applies to
every F figure in this report.

Reading of the numbers (descriptive, n = 1):

- At k=16 (2,247 B) the only material load-fidelity failure is F_A:
  predicted 45,276,627 vs GT 173,798,004 — a 3.84× underprediction
  (analytic-vs-analytic). The verdict is still FAIL (threshold
  10,038,958), so the saturated verdict set hides it entirely; it is
  visible only in Q-error. All other pipelines at k=16 have Q ≤ 1.148.
- At k=64 (6,240 B) every pipeline has Q ≤ 1.011, including F_A
  (1.002).
- At k=256 and k=1024 predictions reproduce every GT load exactly.
  Context: per-column cardinalities are ≤ 262, so at k=256 the
  synopsis retains all but at most 6 keys per column and the residual
  tail does not move any max-partition load after the model's per-key
  rounding; at k=1024 every observed count is stored. These two points
  are therefore near-transcription points on this instance set, not
  evidence about depth behaviour on higher-cardinality data.

## A9 — not run (absence stated per §6)

A9 operating points are **not** included. The end-biased design in
`classical_arms.py` is defined over the synthetic study's dense key
arrays (K = 20,000 baked in, including `NULL_PSEUDO_KEY = K`), its
fixed fact/fact2 column structure (in which the second table's vectors
feed only the F estimator — the real instance set's green-table
pipelines G_C/J_N have no slot), and its own load path. Applying it to
the zone-id count vectors would require redefining the column mapping,
the NULL pseudo-key, and the load path — definition changes — so under
the §6 rule it is omitted rather than adapted.

## Scope limits

One month, one dataset (n = 1), descriptive only; analytic GT vs
analytic predictions throughout (F analytic-vs-analytic); verdict
metrics are saturated (all-FAIL) and carry no discriminating
information — the E7 result is the Q-error/byte profile above, and it
does not support any claim about verdict fidelity or about
higher-cardinality real data.
