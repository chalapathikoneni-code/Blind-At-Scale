# UPDATED NUMBER PROVENANCE — Final Scientific Strengthening campaign

Every new number in the campaign reports, traced to its generating code
and raw artifact. Campaign root:
`~/workspace/blind-at-scale/final_scientific_strengthening/`.
Consolidated rows: `ALL_NEW_RESULTS.csv` (1,209 data rows =
E2 475 + E3 280 + E4 267 + E5 68 + E7 119; **+84 rows appended
2026-11-25 for the EMR return — E1/E6/A9-at-100M, §EMR below**), each row carrying its own
`artifact` column. Prior numbers keep the provenance in
`~/workspace/blind-at-scale/NUMBER_PROVENANCE.md` (unchanged).

## E2 — Information frontier (`results/rows_e2.csv`, 475 rows)

- Code: `code/run_e2.py` (arms from the study's `classical_arms.py` /
  `arms.py`, unmodified estimators). Environment gate: A9 on zipf_s101
  reproduced the recorded Experiment A values **exactly** before any new
  run (`results/e2_*summary*.json`, gate rows in `rows_e2.csv`).
- Bytes are actual serialised packet sizes; per-dataset packets preserved
  (`results/packet_A9_t{16,64,256,1024}_*.json` and A8 equivalents).
  Frontier table (bytes = mean over 9 datasets; Q over 1,089
  instance-pipelines): A9_t16 1,541 B / median Q 3.3449 / max 1070.2;
  A8_k16 2,241 / 1.00103 / 6.410; A9_t64 4,421 / 1.0324 / 267.6;
  A8_k64 6,744 / 1.00004 / 3.166; A9_t256 15,659 / 1.0137 / 66.89;
  A8_k256 24,469 / 1.00007 / 1.745; A7_full 29,801 / 1.0153 / 9.699;
  A7X_full 58,471 / 1.00049 / 1.795; A9_t1024 60,649 / 1.00155 / 16.72;
  A8_k1024 94,460 / 1.00003 / 1.040; A5_full 94,460 / 1.00147 / 2.740.
  Aggregates: `results/metrics_e2.csv`.
- Every frontier point was recomputed fresh in this campaign and matched
  the previously recorded CSVs where they overlap. One CSV-writer bug in
  the campaign code was caught during verification, fixed, and the
  affected outputs regenerated before delivery (recorded in the E2
  report).

## E3 — Near-threshold depth sensitivity (`results/rows_e3.csv`, 280 rows)

- Code: `code/e3_driver.py`; extension generator `code/gen_ext.py`
  (unmodified study generator). Consistency gate:
  `results/e3_consistency_check.json` — 88 instances, 0 GT mismatches
  against the frozen labels before any new arm ran.
- Original grid (denominators 62 GT FAIL / 26 GT PASS): A8@16 recall
  0.871, spec 1.000; A8@64 0.903/1.000; A8@256 0.903/1.000 (same FN set
  as @64); A8@1024 (reused frozen rows) 0.968/1.000; A9@16 recall 1.000,
  spec 0.000; A9@64 1.000/1.000, worst Q 267.6 (G_C); A9@256 worst Q
  66.9; A9@1024 worst Q 16.7. Reused references: A5 62/62 + 26/26, max Q
  2.74; A7 recall 0.984; A1 recall 0.613 (24 FN, all J-family).
- Hash sensitivity (same instances, sensitivity hashes): hash1 @1024 →
  7/88 verdict flips (all FAIL→PASS; J_B 2/8; recall 0.887); hash1 @64
  and hash2 @1024/@64 → 0 flips. Per-instance detail:
  `results/e3_summary.json` + per-instance CSVs in `results/`.
- Extension set (separately frozen; datasets in `data_ext/` only):
  `results/e3_extension_summary.json` — 44 instances (34 FAIL / 10
  PASS); A5, A8@1024, A8@64, A9@1024, A9@64 verdict-perfect; A1 recall
  0.647; A9 worst Q 265.7 @t64 (G_C).

## E4 — Physical validation (`results/rows_e4.csv`, 267 rows)

- Code: campaign event-log re-parse (both EMR logs from scratch);
  detail: `results/e4_analysis_detail.json`; stage aggregates:
  `results/e4_stage_aggregates_synthetic.json`,
  `results/e4_stage_aggregates_realdata.json`.
- Cross-check: 31/31 physical pipeline-instance executed loads match the
  prior verifier's outputs exactly (prior artifacts:
  `~/workspace/blind-at-scale/emr_results_synthetic/`,
  `~/workspace/blind-at-scale/emr_results_realdata/`).
- Headline statistics: Stage C primary set n = 24 non-F physical —
  ratio median 0.9912, mean 0.9645, range 0.8574–1.0353; Spearman ρ =
  0.971; Pearson (log₁₀ loads) = 0.9994; executed load vs max task
  duration ρ = 0.592. Synthetic set: ratio median 1.0000, ρ = 0.953.
- Spill (byte-exact re-derivation): synthetic J_N stage 38 —
  748,309,212 B, 1 task; real-data Stage S J_A 2,517,426,184 B / 2 tasks
  + J_B 1,892,575,254 B / 2 tasks; total 4 tasks, 4,410,001,438 B.
- Figure: `figures/e4_analytic_vs_executed.pdf` / `.png`; machine-
  readable data: `results/e4_figure_data.csv`.

## E5 — Profiling overhead (`results/rows_e5.csv`, 68 rows)

- Code: `code/e5_driver.py`, `code/e5_phase.py`, `code/e5_summarize.py`
  (new code only). Raw per-repetition results: `results/e5_raw.json`;
  driver log: `results/e5_driver.log`. Method: 3 fresh-subprocess
  repetitions per scale, medians reported; environment = realdata venv
  (Python 3.12.3, numpy 2.5.5, pyarrow 26.0.1, DuckDB 2.0.1) on the
  shared VM (AMD EPYC 9D25, 4 visible CPUs; concurrent campaign load —
  contention caveat in the report).
- Medians (s): scan 1.279 / 2.121 / 7.310 / 12.750 at 200K / 1M / 10M /
  40M; build 0.013 / 0.014 / 0.058 / 0.013; serialise 0.0013 / 0.0023 /
  0.0014 / 0.0015; predict A8 0.221 / 0.208 / 0.320 / 0.205; A5 0.289 /
  0.218 / 0.232 / 0.215; A9 0.082 / 0.077 / 0.013 / 0.012; end-to-end
  (scan+build+serialise+A8) 1.514 / 2.345 / 7.690 / 12.969.
- Peak memory at 40M (ru_maxrss delta, median): scan 715.5 MB; build
  1.5 MB; predict A8/A5/A9 = 11.5 / 11.8 / 3.1 MB.
- Validation: scan-derived counts equal `counts.npz` exactly (max abs
  diff 0, all arrays) at all 4 scales × 3 reps; serialised synopsis at
  10M = 91,379 B = on-disk `synopsis.json` size. Serialised bytes by
  scale: 74,887 / 81,412 / 91,379 / 96,514.
- NOT MEASURED (never estimated): refresh cost; deployment/cloud
  profiling cost; testbed materialisation to disk.

## Coordinator verification pass (2026-11-25, before the verdict)

Every headline number above was spot-checked by a **fresh re-read of
the raw artifacts** (not the generating scripts' summaries or the
worker reports):

- **E4** — recomputed from `e4_analysis_detail.json` per-point pairs:
  n = 24, median 0.9912, mean 0.9645, min 0.8574, max 1.0353 — exact
  match. Spill events read from the same raw file: 748,309,212 B
  (synthetic, 1 task); 2,517,426,184 B (real-data J_A) — match.
- **E3** — read from `e3_summary.json` raw points: A8 recalls
  0.871/0.903/0.903/0.968, A9@16 specificity 0.000 (26 FP), A9 max Q
  267.551 @t64 / 66.889 @t256 / 16.723 @t1024, A5 max Q 2.740,
  hash1@1024 = 7 flips / recall 0.8871, consistency 88/0 — all match.
  Cross-experiment consistency: A9@16 max Q 1070.2 appears in both
  E2 and E3 raw outputs (same G_C mechanism).
- **E7** — read from `e7_summary.json`: bytes 2247/6240/19999/20211;
  F_A k16 prediction 45,276,627 vs GT 173,798,004 → Q 3.839 — match;
  consistency_ok = true.
- **E5** — recomputed medians from `e5_raw.json` raw repetitions:
  40M scan [15.288, 11.911, 12.750] → 12.750 ✓; build → 0.013 ✓;
  10M scan median 7.310 ✓; validation_exact = true, all count diffs 0.
- **E2 — DISCREPANCY FOUND AND CORRECTED.** The E2 report's prose
  (§3 table + two passages) stated all 11 points at precision 1.000 /
  FP = 0. Its own machine artifacts (`summary_e2.json`,
  `metrics_e2.csv`, `rows_e2.csv` — three files, mutually consistent)
  give A9_t16 precision 0.6061 / FP 39, A9_t64 and A9_t256 precision
  0.8696 / FP 9. The machine artifacts govern (they are also
  corroborated by E3's independent finding of A9@16 degeneracy). The
  E2 report now carries a dated erratum and corrected cells; all
  downstream campaign documents use the corrected values. No
  load-fidelity, byte, or Pareto number was affected.
- **Contradiction check vs the Nov 24 manuscript / handoff:** no
  campaign number contradicts a verified handoff number. Checked pairs
  include A9 median Q 1.00155 (E2 = Experiment A), A8_k16 max Q 6.410
  (= handoff C21's 6.4097), A8@1024 near-grid recall 0.968 (= C20's
  "misses 2/62"), Stage C non-F median ratio 0.9912 (= real-data
  verification report). E7's A8 byte counts (≤ 20,211 B) differ from
  the synthetic grid's (94,460 B at k1024) because NYC group
  cardinalities are ≤ 262 — different data, not a contradiction; the
  near-transcription caveat travels with the number.

## E7 — Real-data information budget (`results/rows_e7.csv`, 119 rows)

- Code: `code/e7_real_data.py` (synthetic-study A8 definition; packaged
  `realdata_model` for ground truth). Summary:
  `results/e7_summary.json`. Consistency gate: analytic GT for NYC Jan
  2026 reproduced exactly on all 11 pipelines from local parquet before
  scoring.
- A8 bytes at k = 16/64/256/1024: 2,247 / 6,240 / 19,999 / 20,211.
  Verdict agreement 10/10 at every k under both thresholds (denominator
  10 scored; D_A diagnostic excluded; group saturated — all 10 GT FAIL).
  Median Q 1.026 / 1.004 / 1.000 / 1.000; max Q 3.839 (F_A @k16) / 1.011
  / 1.000 / 1.000. F pipelines labelled analytic-vs-analytic throughout.

## EMR return — E1 / E6 / A9 at 100M (appended 2026-11-25, 84 rows)

Source: the author's capture `bas_final_capture_20261125-192309Z.zip`
(Drive EM_Results, md5 `43b51cf904494b5cd89a2369c48270a4`, Drive =
local), unzipped at `~/workspace/blind-at-scale/emr_results_final_capture/`.
Full verification: `EMR_FINAL_CAPTURE_VERIFICATION.md` (this folder);
parser `code/parse_eventlogs_emr_final.py`; parsed log summary
`results/emr_final_capture_eventlog_summary.json`; A9 scoring
`results/a9_100m_scoring.json`. Code that ran: the delivered
`emr_package_final/code/` (unpatched originals; step commands +
confs extracted from the captured controller logs).

- **Placement/execution**: all 5 captured applications (smoke, prep,
  A9-100M, E6-synth off/on) re-derived from their event logs: 18
  executors / 18 distinct hosts each, 0 failed tasks, Spark
  3.5.8-amzn-0. The 3 crashed cluster-A applications' event logs were
  NOT in the capture (finding; failure evidence is step-log derived).
- **E1 join_dim (4 parts)**: measured max 51,961 / output 5,368,834 /
  PASS-PASS recomputed from `capture/output/e1_join_dim.json` (loads
  sum exactly). Arm Qs vs measured max: synopsis 1.0217 (pred
  50,856), endbiased 1.0519 (54,656), uniform 2.3094 (120,000,
  FAIL/FAIL = false positive). join_dim_s8 (Step 6 partial): max
  89,773 / 10,218,360; Qs 1.0264 / 1.0338 / 5.6810 (uniform FP);
  event-log cross-check pending Step 6 return.
- **E1 selfjoin bounds**: 11,056² = 122,235,136 (single-key output,
  from `prep_summary.json` top_author_counts); Σcount² ≥ 624,940,052
  (top-30 square-sum, computed in the verification pass); frozen
  limits at that bound 12,498,801 (abs) / 9,764,688 (rel); ratio vs
  input-side max 51,961 = 2,352.44. Failure classification from the
  three step logs (timestamps, executor-loss lines, exit codes quoted
  in the verification report §6).
- **E6**: join_dim off/on JSONs (max 51,961 both; arrays bit-identical
  and identical to Step 2's). Synth pair from the JSONs + event logs:
  J_A max 10,873,845, J_N max 10,442,419 (both conditions); app
  shuffle totals 5,039,703,273 B / 202,876,741 records identical in
  both apps; spill 0. Reconciliation vs Nov GT: each executed maximum
  is exactly 154 records below the GT shuffle-read load (dim-side
  records in the fullest task).
- **A9 at 100M**: predictions from `capture/output/a9_100m.json`
  (packet 78,447 B, serialised JSON of thresholds + retained
  keys/counts for 6 columns + null entry); scored against
  `emr_results_synthetic/round2/emr_verify_out/emr_ground_truth_100m.json`
  (Nov 23 executed GT; counts SHA-256 3549c275… matches the anchor).
  Verdicts 9/9; per-pipeline Q vs gt_load: F_A 1.000030, F_B 1.011576,
  G_A 1.140069, G_B 2.034757, G_N 1.139873, J_A 1.026908, J_B
  1.215471, J_N 1.031828, W_A 1.140069 → median 1.139873, mean
  1.193398. Q vs max_group_count (G only): 1.000000 ×3. G_C predicted
  (97,657, PASS) but unscored — no executed GT in the 9-pipeline set.
  A9 app inputs from its event log: 4 × 100M + 2 × 60M = 520M records.

## Final return 2026-11-26 (COMPLETE capture; md5 c126977ec0ad573a75288c6415deda4d)

All values recomputed from `emr_results_final_return/capture/` raw
artifacts; see `VERIFICATION_REPORT_FINAL_RETURN.md` (uploaded with
this folder).

- **join_meta (4-part)**: measured max 43,348, output 5,368,834
  (`output_Final/e1_join_meta.json`, measured block; partition loads
  sum exactly to output rows). Q (fold ratio): synopsis 43348/43187 =
  1.0037; end-biased 47937/43348 = 1.1059; uniform 110000/43348 =
  2.5376 (false positive). E6 pair files' load arrays are identical
  to this file's measured array, element by element.
- **Selfjoin exact predictions**: 4-part synopsis predicted output =
  Σcount² = 795,242,814, predicted max load 122,469,088
  (`output_Final/e1_selfjoin.json`); satisfies the earlier top-30
  bound ≥ 624,940,052 (prep top_author_counts, 4-part prep_summary,
  byte-identical across both captures). 1-part: 262,464,138 /
  43,344,257 (`output_Final_1part/e1_selfjoin.json`; 1-part prep
  fact_rows 1,260,852, top author 6,582).
- **join_source_dim**: measured max 171,026, output 1,405,314
  (`output_Final/e1_join_source_dim.json`); Q synopsis 173024/171026
  = 1.0117, end-biased 172099/171026 = 1.0063, uniform 420000/171026
  = 2.4558. selfjoin_source: Σcount² 44,878,025,678, predicted max
  27,160,194,356 (`output_Final/e1_selfjoin_source.json`).
- **join_dim_s8 event-log tie**: Step 6 application
  <APPLICATION_ID> stage 18 shuffle records written
  10,218,360 = `e1_join_dim_s8.json` measured output_rows (file
  md5-identical in both captures: 27d1a9cbf7a35a710c315a01a692955d).
- **Recovery event logs** (cluster <CLUSTER_ID>, apps
  _0006_1…_0013_1): 18 hosts, 0 failed tasks, 0 spill each; join_meta
  AQE pair shuffle write 257,923,472 B in both _0009_1 (off) and
  _0010_1 (on).

---

## Measured self-join — 2026-11-26 (VERIFICATION_REPORT_SELFJOIN.md)

Source artifacts: `emr_results_selfjoin_measured/capture/
measured_json/e1_selfjoin_measured_s1.json` (md5
6a59385416aeb9adb2c8e7fd38797749) and `e1_selfjoin_measured_s4.json`
(md5 7ac3c12f8270ae59cd5d3d36fe9cb709); event logs
`capture/event_logs/run1_s1_<APPLICATION_ID>_1` (md5
7fce3f0d6f343f78d62f19ab9e3d8546) and
`run2_s4_<APPLICATION_ID>_1` (md5
d65114bda2de4c8121f179ea6aa1f9b6). Cluster
<CLUSTER_ID>, Spark 3.5.8-amzn-0, AQE off, 128
partitions, `--force-measure` (guard overridden, predictions
recorded first).

- 1 part: measured output rows 262,464,138 (= predicted Σcount²,
  diff 0); measured max partition load 43,340,706; predicted max
  43,344,257; Q (pred/meas) 1.000082; median partition load
  217,340; verdict FAIL/FAIL measured; exceedances 8.26× (abs
  limit 5,249,282.76) / 10.57× (rel limit 4,101,002.16); arm Qs:
  end-biased 1.024735, uniform 1.155726; 453 tasks, 0 failed,
  18 hosts, spill 0; elapsed 198 s (app 216 s).
- 4 parts: measured output rows 795,242,814 (= predicted Σcount²,
  diff 0); measured max partition load 123,158,197; predicted max
  122,469,088; Q 0.994405; median 1,519,986.5; verdict FAIL/FAIL
  measured; exceedances 7.74× (abs 15,904,856.28) / 9.91× (rel
  12,425,668.97); arm Qs: end-biased 1.030343, uniform 0.727114;
  608 tasks, 0 failed, 18 hosts, spill 0; elapsed 387 s
  (app 405 s).
- Per-partition arrays (128 values each) are inside the JSONs;
  sums and maxima re-verified against the reported aggregates.
- Script diff (G03, closed 2026-11-26): patched campaign scripts
  vs `emr_package_final/code/` — common.py additions only
  (guard constants + streaming `count_per_partition`);
  `predict_from_counts` and `verdict` byte-identical; e1_join /
  e1_more / e6_aqe changes limited to streaming counting, guard
  extension + recording, `--cases` selection, and the
  `--force-measure` override. **Prediction math unchanged.**
- E5 peak-memory clarification (audit R8): the 40M scan reps in
  `e5_raw.json` are 725.1 / 715.5 / 692.5 MB; 715.5 MB is the
  median rep and the observed peak is **725.1 MB**. Peak
  statements use 725.1 MB from the final revision on.
