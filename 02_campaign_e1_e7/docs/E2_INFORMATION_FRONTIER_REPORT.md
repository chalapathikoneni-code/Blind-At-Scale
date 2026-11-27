# E2 — Symmetric Information-Budget Frontier (Headline 99)

> **ERRATUM (coordinator verification, 2026-11-25).** As first
> delivered, §3's table and two prose passages stated that all 11
> frontier points achieve precision 1.000 / FP = 0 on the 99. That was
> a reporting error in the prose, contradicted by this experiment's
> own machine artifacts (`results/summary_e2.json`,
> `results/metrics_e2.csv`, `results/rows_e2.csv` — all three agree).
> Correct values: A9_t16 precision 0.606, FP = 39 (degenerate);
> A9_t64 and A9_t256 precision 0.870, FP = 9 each; the other eight
> points are FP = 0 as stated. The table and prose have been corrected
> in place; no load-fidelity, byte, or Pareto result is affected. The
> error was caught by an independent re-read of the raw artifacts
> against the report before the campaign verdict was written.

Final Scientific Strengthening campaign, protocol §2 (+ §7 statistics).
Executed 2026-11-25, local, system python3 3.12.3 / numpy 1.26.4,
cwd `blind-at-scale/code/`, unmodified study code. Driver:
`final_scientific_strengthening/code/run_e2.py`. Machine-readable
outputs: `final_scientific_strengthening/results/metrics_e2.csv`
(per-point uniform metrics), `results/summary_e2.json`,
`results/verification_e2.json`, `results/rows_e2.csv`, plus
per-instance prediction CSVs and packets for the new A9 runs
(`results/predictions_A9_t{16,64,256}_<tag>.csv`,
`results/packet_A9_t{16,64,256}_<tag>.json`).

Datasets: the 9 headline datasets {zipf, lognormal, mixed} × seeds
{101, 202, 303}, 10M fact rows each; 99 instances (9 × 11 pipelines);
GT FAIL = 60, GT PASS = 39. Q-error is over 99 × 11 = 1,089
instance-pipelines. Per §7, the independent unit is the dataset
(n = 9); instance-level figures are descriptive over correlated
instances. Classification of all E2 rows in `rows_e2.csv`:
**simulated** (analytic prediction from profiler counts/synopses; no
physical execution).

## 1. Environment validation gate — PASS

Before any new run: A9 on zipf_s101 recomputed via
`run_exp_a.run_a9` → `classical_arms.predict_A9` through the
`run_exp_a.py` load path (counts from `counts.npz`, ground truth
recomputed and cross-checked against `predictions_main.csv`).
Result: 11/11 instance-pipelines compared, **0 pred_load mismatches**
at the recorded 0.1 rounding, 0 verdict mismatches, GT cross-check
0 mismatches. The gate passed, so the new runs below count.

## 2. Coordinated-sampling determination — A9 IS the coordinated baseline

Inspection of `code/classical_arms.py`:

- **(i) One hash/seed shared across all tables and columns — YES.**
  `sampling_hash` (line 46) computes h_H(v) = splitmix64(v + 1 + H)/2^64
  with default `hseed=HASH_SEED_HEADLINE = 0` (line 38). `build_packet`
  (line 110) calls `build_sample(..., hseed=hseed, target=target)` with
  the *same* hseed for every fact column (line 121) and every fact2
  column (line 129), and the NULL pseudo-key is hashed with the same
  seed (line 141). Retention decisions for a key are therefore
  consistent across tables — this is exactly the paper's coordination
  (module docstring, lines 14–16).
- **(ii) Exact retention of f_v ≥ threshold — YES.** `build_sample`
  line 91: `head = c >= threshold`; retained head values store their
  exact frequency (line 92 ff.; packet stores `counts` verbatim).
- **(iii) Hash-probabilistic tail retention — YES.** Line 92:
  `retained = head | (h <= c / threshold)` — a tail value v is retained
  with probability f_v/T under the shared hash, and if retained is
  Horvitz–Thompson-reconstructed as T.

All three properties hold. **Determination: A9 (end-biased sampling,
Estan–Naughton 2006) IS the coordinated hash-consistent join-key
sampling baseline with heavy-hitter retention. No redundant baseline
was built.**

## 3. Frontier table (uniform metric set, every point recomputed fresh)

Verdict fidelity (denominator 99; GT FAIL 60 / PASS 39) and load
fidelity (denominator 1,089) are reported separately. Kind: T =
runnable testbed, D = direct predictor (protocol §2 classification).
Bytes: mean (min–max) over the 9 datasets. Prediction time: mean
wall seconds per dataset for the prediction call + `loads_from_counts`,
this environment.

| Point | Kind | Bytes mean (min–max) | Recall | Prec | F1 | FP | FN | Median Q | Mean Q | p95 Q | Max Q | Worst underpred. (60 GT-FAIL) | Pred. time (s) | Testbed rows (mean) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A9_t16 | D | 1,541 (1,405–1,636) | 1.000 | 0.606 | 0.755 | 39 | 0 | 3.3449 | 90.113 | 1040.32 | 1070.21 | 1.0014 | 0.029 | 0 |
| A8_k16 | D | 2,241 (2,168–2,336) | 1.000 | 1.000 | 1.000 | 0 | 0 | 1.00103 | 1.162 | 1.420 | 6.410 | 6.410 | 0.107 | 0 |
| A9_t64 | D | 4,421 (4,014–4,772) | 1.000 | 0.870 | 0.930 | 9 | 0 | 1.0324 | 23.276 | 260.08 | 267.55 | 1.0025 | 0.057 | 0 |
| A8_k64 | D | 6,744 (6,523–7,174) | 1.000 | 1.000 | 1.000 | 0 | 0 | 1.00004 | 1.079 | 1.196 | 3.166 | 3.166 | 0.111 | 0 |
| A9_t256 | D | 15,659 (14,597–17,074) | 1.000 | 0.870 | 0.930 | 9 | 0 | 1.0137 | 6.476 | 65.02 | 66.89 | 1.0122 | 0.048 | 0 |
| A8_k256 | D | 24,469 (23,710–25,894) | 1.000 | 1.000 | 1.000 | 0 | 0 | 1.00007 | 1.039 | 1.152 | 1.745 | 1.745 | 0.103 | 0 |
| A7_full | T | 29,801 (25,999–33,781) | 1.000 | 1.000 | 1.000 | 0 | 0 | 1.0153 | 1.595 | 7.857 | 9.699 | 1.1127 | 0.063 | 101,512 |
| A7X_full | D | 58,471 (55,015–63,423) | 1.000 | 1.000 | 1.000 | 0 | 0 | 1.00049 | 1.053 | 1.602 | 1.795 | 1.0065 | 0.062 | 0* |
| A9_t1024 | D | 60,649 (58,601–64,382) | 1.000 | 1.000 | 1.000 | 0 | 0 | 1.00155 | 2.265 | 16.256 | 16.723 | 1.0017 | 0.038 | 0 |
| A8_k1024 | D | 94,460 (91,351–100,308) | 1.000 | 1.000 | 1.000 | 0 | 0 | 1.00003 | 1.005 | 1.027 | 1.040 | 1.0230 | 0.089 | 0 |
| A5_full | T | 94,460 (91,351–100,308) | 1.000 | 1.000 | 1.000 | 0 | 0 | 1.00147 | 1.118 | 2.330 | 2.740 | 1.0235 | 0.158 | 100,000 |

(A9 byte means: t16 1,540.9, t64 4,420.6, t256 15,659.4,
t1024 60,648.9. *A7X is classified a
direct predictor by the protocol; its predictor internally samples
tail rows at a per-dataset rate and its recorded
`testbed_fact_rows` mean is 114,367 — reported here for transparency,
not counted as a testbed.)

**Verdict fidelity — corrected (see ERRATUM below).** Eight of the
11 points (A8 at all depths, A5, A7, A7X, A9_t1024) have recall =
precision = F1 = 1.000 on the 99. The A9 low-depth points do **not**:
A9_t16 is degenerate (precision 0.606, FP = 39 — it predicts FAIL for
all 99 instances), and A9_t64 / A9_t256 each produce 9 FPs (precision
0.870). All points have recall 1.000 (FN = 0): A9's verdict errors are
exclusively false alarms from load overprediction, never concealed
failures. Verdict metrics therefore discriminate the A9 low-depth
points (negatively) but not the remaining eight; load fidelity
discriminates throughout.

### Per-family Q-error (median / max; denominators: G 36, J 27, F 18, W 9, D 9 instance-pipelines)

| Point | G | J | F | W | D |
|---|---|---|---|---|---|
| A8_k16 | 1.000 / 1.000 | 1.061 / 1.640 | 1.028 / 6.410 | 1.000 / 1.000 | 1.001 / 1.002 |
| A8_k64 | 1.000 / 1.000 | 1.039 / 1.388 | 1.004 / 3.166 | 1.000 / 1.000 | 1.001 / 1.002 |
| A8_k256 | 1.000 / 1.000 | 1.043 / 1.156 | 1.005 / 1.745 | 1.000 / 1.000 | 1.001 / 1.002 |
| A8_k1024 | 1.000 / 1.000 | 1.009 / 1.040 | 1.001 / 1.023 | 1.000 / 1.000 | 1.001 / 1.002 |
| A9_t16 | 5.832 / 1070.2 | 3.563 / 11.55 | 2.653 / 7.47 | 3.346 / 3.56 | 1.182 / 1.20 |
| A9_t64 | 1.513 / 267.6 | 2.151 / 4.33 | 1.417 / 7.21 | 1.000 / 1.000 | 1.001 / 1.000 |
| A9_t256 | 1.000 / 66.89 | 1.170 / 2.53 | 1.016 / 2.20 | 1.000 / 1.000 | 1.001 / 1.000 |
| A9_t1024 | 1.000 / 16.72 | 1.085 / 1.35 | 1.007 / 1.11 | 1.000 / 1.000 | 1.001 / 1.002 |
| A5_full | 1.000 / 2.740 | 1.009 / 1.071 | 1.000 / 1.023 | 1.000 / 1.000 | 1.001 / 1.002 |
| A7_full | 1.022 / 9.699 | 1.013 / 1.132 | 1.017 / 1.113 | 1.019 / 1.038 | 1.005 / 1.012 |
| A7X_full | 1.000 / 1.795 | 1.002 / 1.026 | 1.000 / 1.004 | 1.000 / 1.000 | 1.001 / 1.002 |

The A8 weak spot is F (fact–fact): worst-case Q 6.41 at k16 and
3.17 at k64, both underpredictions of GT-FAIL instances (the F_B
mixed-family cases; worst underprediction = max Q for A8 because the
extremes are underpredictions). The A9 weak spot is the opposite
direction: its extremes are **over**predictions, concentrated in G —
at every target the single worst instance is G_C on a GT-PASS
instance (Q 1070.2 / 267.6 / 66.9 / 16.7 at t16/64/256/1024; the
HT reconstruction imputes the full threshold T to a retained tail
value, inflating a small true max count). A9's worst
*under*prediction on GT-FAIL instances stays ≤ 1.012 at every
target: on this grid A9 never conceals a failure, it inflates
passing loads.

## 4. Pareto frontiers (§2 definition: ≤ on both axes, < on one)

- **(bytes ↓, median Q ↓):** A9_t16 (1,541 B, 3.3449) → A8_k16
  (2,241 B, 1.00103) → A8_k64 (6,744 B, 1.00004) → A8_k1024
  (94,460 B, 1.00003).
- **(bytes ↓, max Q ↓):** A9_t16 (1,541 B, 1070.2) → A8_k16
  (2,241 B, 6.410) → A8_k64 (6,744 B, 3.166) → A8_k256
  (24,469 B, 1.745) → A8_k1024 (94,460 B, 1.040).

A9_t16 sits on both frontiers **only because it is the cheapest
point** — frontier membership at the extreme of one axis is by
construction, and its load fidelity (median Q 3.34, mean Q 90.1,
max Q 1070) is the worst of any point. It is not a fidelity result.

**Plain statements (no dominance claimed from asymmetric points or
pooled medians alone — the comparisons below are point-to-point on
both axes):**

- **No A9 point matches or beats A8 at comparable bytes on load
  fidelity.** A8_k16 (2,241 B) dominates A9_t64, A9_t256 and
  A9_t1024 outright: fewer bytes *and* lower median Q *and* lower
  max Q than each. A8_k64 (6,744 B; median Q 1.00004, max Q 3.166)
  likewise dominates A9_t256 and A9_t1024. At the one byte range
  where A9 is cheaper than any A8 rung (t16, 1,541 B), its median Q
  is 3.34 against A8_k16's 1.001 at 2,241 B.
- The A9 depth sweep is steeply degrading: median Q 1.0016 →
  1.0137 → 1.0324 → 3.3449 and max Q 16.7 → 66.9 → 267.6 → 1070.2
  as target goes 1024 → 256 → 64 → 16. Even at its full operating
  point (t1024, 60,649 B), A9's mean Q (2.265) and max Q (16.72)
  are worse than A8_k16's (1.162 / 6.410) at 27× fewer bytes; A9's
  earlier headline standing rested on median Q and verdicts, both
  of which saturate here.
- A5 vs A8 at identical side-information bytes (94,460 B, the same
  synopsis): the direct predictor A8 has better load fidelity
  (median Q 1.00003, max 1.040) than the testbed A5 (1.00147,
  max 2.740); A5 additionally materialises a 100,000-row testbed
  (mean over datasets; reported separately, never counted in
  side-information bytes).
- A7 (testbed, 29,801 B packet + 101,512 mean testbed rows) has
  the worst max Q (9.699, family G) of any full operating point
  except A9_t1024. A7X (direct, 58,471 B) is mid-pack: median Q
  1.00049, max Q 1.795 — better tail behaviour than A9_t1024 at
  fewer bytes, worse than every A8 rung on max Q.

## 5. Byte accounting — what the bytes buy (per point)

Conventions (frozen, the study's existing ones; the difference is
reported, not hidden): **synopsis family** (A8, A5) = `len(json.dumps(obj))`
with default separators — the on-disk `synopsis.json` format
(verified: default-separator dumps length equals the on-disk file
size on 9/9 datasets). **Packet family** (A9, A7, A7X) = file size
of the compact-separator (`,`, `:`) packet JSON. The convention gap
is material: the zipf_s101 synopsis is 91,379 B under the synopsis
convention vs 74,936 B if compact-serialised (−18%). Cross-family
byte comparisons inherit this asymmetry; it favours the packet
family, and the A8 points still dominate as shown above.

- **A8_k{16,64,256,1024}** (synopsis): per fact column (kA, kB, kC,
  kN): k heavy-hitter (key, exact count) pairs + column total,
  cardinality, tail_mass (kN also null_mass); per pair column (kA,
  kB): k aligned triples (key, fact count, fact2 count) — this is
  the pair/alignment information — plus k fact2 heavy-hitter counts
  and both tail masses; plus `dim_rows` = K (dimension size; the dim
  itself is not stored). No tail *shape* is stored: the power-law
  tail is refit at prediction time from the stored HH counts and
  tail_mass. Retained rows: 0.
- **A9_t{16,64,256,1024}** (packet): header `hash_seed` +
  `target_retained` (hash/target metadata); per column (fact
  kA/kB/kC/kN, fact2 kA/kB): threshold T, column total, and the
  retained (key, exact count) pairs — mean retained pairs per
  packet across the 6 columns: 111.8 (t16), 418.3 (t64), 1,618.6
  (t256), 6,437.1 (t1024); plus a NULL entry (count, threshold,
  retained flag). No explicit pair/alignment block and no tail
  statistics beyond T and the total: join alignment is implicit in
  the shared hash (same key retained in both tables), and the tail
  is handled by HT imputation (retained tail value → T). Retained
  rows: 0 (key+count pairs only).
- **A5_full**: the full A8_k1024 synopsis (same bytes) **plus** a
  runnable testbed of 100,000 fact rows (mean; r = 0.01 sampling of
  the reconstruction). Testbed rows are a separate resource, never
  conflated with the 94,460 side-information bytes.
- **A7_full** (testbed): packet holds, per column, the 1,024
  heavy-hitter key **ids only (no counts)**, column total and
  tail_mass, for fact and fact2; plus `dim_rows`. The fidelity
  comes from the runnable testbed: 101,512 sampled fact rows
  (mean), HH-stratified (β = 0.9). Packet bytes and testbed rows
  reported separately.
- **A7X_full** (direct): packet holds, per column, 1,024
  heavy-hitter (key, exact count) pairs + total + tail_mass for
  fact and fact2; no pair/alignment block (fact2 HH are fact2's own
  top-1024). Prediction is exact on HH keys and HT on a sampled
  tail (see the testbed-rows footnote in §3).

## 6. Verification (all fresh recomputes vs recorded artifacts)

Every point was recomputed fresh in this environment and compared
against the existing per-instance CSVs at the recorded 0.1
rounding (`results/verification_e2.json`):

| Point | Source CSV | Compared | Pred mismatches | Verdict mismatches |
|---|---|---|---|---|
| A8_k16/64/256 | `results/ablation/predictions_hh_k*.csv` | 99 each | 0 | 0 |
| A8_k1024 | `results/ablation/predictions_full_k1024.csv` | 99 | 0 | 0 |
| A5_full | `data/<tag>/predictions_main.csv` (seed = ds·1000+73, run_arms convention, arms order A1,A2,A3,A4,A5,A8) | 99 | 0 | 0 |
| A7_full / A7X_full | `data/<tag>/predictions_eqinfo.csv` (seed = ds·1000+5000+idx, run_eqinfo path) | 99 each | 0 | 0 |
| A9_t1024 | `results/exp_a/predictions_exp_a_<tag>.csv` | 99 | 0 | 0 |

Byte-side checks: rebuilt A7 and A7X packets serialise
byte-identically to the existing packet files on 9/9 datasets each;
A9_t1024 packet sizes match the existing `packet_A9_<tag>.json`
file sizes on 9/9; synopsis default-separator dumps length equals
on-disk `synopsis.json` size on 9/9. Ground truth was recomputed
from `counts.npz` per dataset and cross-checked against
`predictions_main.csv`: 0 verdict mismatches on all 9 datasets.
A8/A9 are deterministic (A9 hash-determined; A8's r=None path
consumes no RNG), so exact reproduction was required and obtained;
A5/A7/A7X reproduce exactly under their original seed conventions.
The uniform metrics in §3 are computed from the fresh recomputes;
for every pre-existing point they agree with the recorded
summaries (e.g. frontier.csv median Q values) to the precision
shown. Nothing was computed solely from existing CSVs — no point
needed that fallback.

## 7. Negatives, stated plainly

- The A9 depth sweep does **not** produce a cheap classical point
  competitive with A8: below t1024 its load fidelity collapses
  (median Q 3.34 and max Q 1070 at t16), and at t1024 it is
  dominated by A8_k16 at 27× fewer bytes.
- Verdict fidelity is saturated for eight of the 11 points
  (99/99); the exceptions are the A9 low-depth points, which E2
  discriminates **against** on verdicts (A9_t16 degenerate, FP = 39;
  A9_t64/t256 FP = 9 each). No verdict-superiority claim for any
  point should be hung on this grid.
- A9's large Q-errors are overpredictions on GT-PASS instances
  (G_C worst at every target); A8's are underpredictions on
  GT-FAIL F instances. The two arms' failure modes are in opposite
  directions and are not interchangeable in a pooled median.
- Prediction wall times are single-run measurements on a shared
  VM (this run: A8 ≈ 0.09–0.11 s, A9 ≈ 0.03–0.06 s, A5 ≈ 0.16 s,
  A7/A7X ≈ 0.06 s per dataset); they are descriptive, not a
  benchmark.

## 8. Artifacts

- Code: `final_scientific_strengthening/code/run_e2.py`
- `final_scientific_strengthening/results/metrics_e2.csv` — uniform
  per-point metrics (incl. per-family median/max Q, Pareto flags)
- `final_scientific_strengthening/results/summary_e2.json`,
  `verification_e2.json`, `run_e2.log`
- `final_scientific_strengthening/results/predictions_A9_t{16,64,256}_<tag>.csv`
  (27 files) and `packet_A9_t{16,64,256}_<tag>.json` (27 files) —
  the new A9 runs
- `final_scientific_strengthening/results/rows_e2.csv` — long-format
  rows (header: experiment,dataset,scale,environment,metric,value,
  denominator,classification,artifact,notes;
  classification = simulated; environment = "local, python
  3.12.3/numpy 1.26.4")

No existing file was modified; the manuscript was not touched.
Nothing in E2 was left unexecuted.
