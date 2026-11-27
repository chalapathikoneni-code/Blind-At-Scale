# E4 — Physical Execution Validation from Existing EMR Event Logs

Executed 2026-11-25, per `FINAL_EXECUTION_PROTOCOL.md` §4 and §7.
Environment of the original runs: AWS EMR (user-executed); logs
re-analysed locally with system python3. Classification throughout:
log-derived numbers are **measured execution**; F-pipeline numbers are
**analytical**. No existing file was modified; new code is in
`final_scientific_strengthening/code/` (`e4_parse_logs.py`,
`e4_analyse.py`), outputs in `results/` and `figures/`.

## 1. Method and stage→pipeline attribution

Both Spark event logs were streamed line by line (never loaded whole):
synthetic `<APPLICATION_ID>` (36 MB) and real-data
`<APPLICATION_ID>` (187 MB). From `SparkListenerTaskEnd`
events we aggregated, per stage: task count, failed tasks, Executor Run
Time distribution, Shuffle Read Metrics (records; remote+local bytes),
Shuffle Write Metrics bytes, and Memory+Disk Bytes Spilled.

**Attribution method (no mapping was invented).** A pipeline's
*principal shuffle stage* is the stage the campaign's own verifier
recorded as `gt_load_stage_id` in the ground-truth JSON
(`emr_ground_truth_100m.json` / `realdata_ground_truth.json`) — i.e.
the stage whose maximum per-task shuffle-read records define the
pipeline's executed load. Every number below is taken from that
verifier-recorded stage. **Validation of the attribution:** for all 31
physical pipeline-instances in the analysis sets (7 synthetic + 24
real-data Stage C), the max shuffle-read records re-derived from the
raw log for the recorded stage **exactly equal** the verifier's
`gt_load` (31/31). Where a number comes from the verifier rather than
the raw log it is labelled *verifier-derived*; all stage metrics here
are *log-derived* and agree with the verifier wherever both exist.

**Analytic predicted load.** Real-data: `analytic_load` in the
ground-truth JSON (the real-data model's xxhash64→splitmix64 load from
per-key counts). Synthetic: the verifier-recorded **D8 load** — the
in-job partition-id load computed from the same counts — since the
synthetic ground-truth JSON records no separate `analytic_load` field
for physical pipelines. This is stated, not hidden: the synthetic
analytic and executed loads use the same partitioner by design, so
synthetic ratios near 1.000 are expected and carry less independent
information than the real-data ratios (different hash family).

**Caveat on shuffle write bytes.** The principal stage is the reduce
side of its shuffle; its own shuffle-write bytes (reported in the
tables) are the small writes feeding the *next* stage, not the bytes
of the shuffle it reads. The shuffle feeding it was written by the
preceding map stage. We report the recorded principal-stage values
verbatim rather than re-attributing map-stage writes.

**Totals re-derived from the logs:** synthetic 5,036 task ends, 0
failed, 18 executor hosts; real-data 25,377 task ends, 0 failed, 18
executor hosts — both exactly matching the verifier/audit records.

## 2. PRIMARY set — real-data Stage C, 24 physical comparisons

3 months (NYC_TLC_2026-01/02/03) × {G_A, G_B, G_C, G_N, J_A, J_B, J_N,
W_A}; analytic vs executed (max per-task shuffle-read records):

- **Ratio executed/analytic: median 0.9912, mean 0.9645, range
  0.8574–1.0353 (n = 24).** This reproduces the audit's non-F median
  (0.9912) exactly; the audit's pooled Stage C median 0.9961 included
  the 6 analytic F points at ratio 1.0.
- **Spearman ρ = 0.971; Pearson on log10 loads = 0.9994 (n = 24).**
- Per-month pattern: G_N ratios are exactly 1.0000 in all 3 months;
  G_B/J_B are the systematic under-executions (0.857–0.888); G_A/J_A/W_A
  sit at 0.949–1.035. Full per-point table: `results/e4_figure_data.csv`
  and `results/e4_analysis_detail.json`.
- Executed load vs principal-stage max task duration: Spearman ρ =
  0.592 (n = 24, descriptive only — pipeline instances are correlated
  within a month; §7 unit is the dataset, here n = 3 months).
- **Spill: zero in all 24 principal stages** (and zero in every Stage C
  stage), so no executed-load-vs-spill association is derivable for
  Stage C — reported as NOT DERIVABLE (no variance), not as zero
  correlation.

Principal-stage detail (log-derived; durations = Executor Run Time):

| Month | Pipe | Stage | Analytic | Executed | Ratio | Shuffle read B | Dur max ms | Dur med ms | Max/Med | Spill B |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-01 | G_A | 14 | 199,975 | 207,031 | 1.0353 | 189,327,602 | 1,488 | 286.0 | 5.20 | 0 |
| 2026-01 | G_B | 21 | 191,226 | 168,975 | 0.8836 | 189,268,007 | 1,135 | 132.0 | 8.60 | 0 |
| 2026-01 | G_C | 28 | 10,596 | 10,547 | 0.9954 | 2,111,396 | 344 | 72.5 | 4.74 | 0 |
| 2026-01 | G_N | 35 | 2,390,495 | 2,390,495 | 1.0000 | 197,883,257 | 8,000 | 23.0 | 347.83 | 0 |
| 2026-01 | J_A | 43 | 199,975 | 207,036 | 1.0353 | 189,334,101 | 2,424 | 390.0 | 6.22 | 0 |
| 2026-01 | J_B | 53 | 191,226 | 168,978 | 0.8837 | 189,274,506 | 1,607 | 263.5 | 6.10 | 0 |
| 2026-01 | J_N | 63 | 10,596 | 10,549 | 0.9956 | 2,117,784 | 239 | 55.0 | 4.35 | 0 |
| 2026-01 | W_A | 80 | 199,975 | 207,031 | 1.0353 | 2,226,922 | 204 | 53.0 | 3.85 | 0 |
| 2026-02 | G_A | 99 | 187,622 | 178,114 | 0.9493 | 173,020,224 | 664 | 63.0 | 10.54 | 0 |
| 2026-02 | G_B | 106 | 179,135 | 153,599 | 0.8574 | 172,870,838 | 707 | 81.0 | 8.73 | 0 |
| 2026-02 | G_C | 113 | 9,772 | 9,733 | 0.9960 | 1,957,015 | 72 | 18.0 | 4.00 | 0 |
| 2026-02 | G_N | 120 | 2,162,640 | 2,162,640 | 1.0000 | 180,895,107 | 6,134 | 9.0 | 681.56 | 0 |
| 2026-02 | J_A | 128 | 187,622 | 178,117 | 0.9493 | 173,026,723 | 818 | 103.5 | 7.90 | 0 |
| 2026-02 | J_B | 138 | 179,135 | 153,602 | 0.8575 | 172,877,337 | 863 | 98.5 | 8.76 | 0 |
| 2026-02 | J_N | 148 | 9,772 | 9,735 | 0.9962 | 1,963,353 | 164 | 24.5 | 6.69 | 0 |
| 2026-02 | W_A | 165 | 187,622 | 178,114 | 0.9493 | 2,059,138 | 73 | 11.0 | 6.64 | 0 |
| 2026-03 | G_A | 184 | 215,595 | 212,796 | 0.9870 | 201,392,967 | 806 | 48.5 | 16.62 | 0 |
| 2026-03 | G_B | 191 | 205,013 | 181,924 | 0.8874 | 201,348,417 | 634 | 65.5 | 9.68 | 0 |
| 2026-03 | G_C | 198 | 11,295 | 11,257 | 0.9966 | 2,303,005 | 77 | 13.0 | 5.92 | 0 |
| 2026-03 | G_N | 205 | 2,719,036 | 2,719,036 | 1.0000 | 210,511,076 | 6,566 | 7.0 | 938.00 | 0 |
| 2026-03 | J_A | 213 | 215,595 | 212,801 | 0.9870 | 201,399,466 | 894 | 75.5 | 11.84 | 0 |
| 2026-03 | J_B | 223 | 205,013 | 181,927 | 0.8874 | 201,354,916 | 992 | 83.5 | 11.88 | 0 |
| 2026-03 | J_N | 233 | 11,295 | 11,259 | 0.9968 | 2,309,337 | 64 | 16.0 | 4.00 | 0 |
| 2026-03 | W_A | 250 | 215,595 | 212,796 | 0.9870 | 2,339,166 | 47 | 8.0 | 5.88 | 0 |

Every principal stage has
128 tasks and 0 failed tasks. G_N's extreme max/median duration ratios
reflect a near-empty median task (most partitions carry almost no
rows; one carries the hot key) — descriptive, not an anomaly claim.
Principal-stage shuffle write bytes are 448–6,384 B per stage (see
caveat in §1); full values in `results/rows_e4.csv`.

### Analytic F comparisons (kept separate, never pooled)

6 comparisons (F_A, F_B × 3 months): analytic loads 148,623,071–
359,123,568. These pipelines were not materialised (D4); there is no
executed load, and the executed/analytic ratio is 1.0 **by
construction**. No association statistic is computed over them.

## 3. Synthetic set (reported separately)

7 physical pipelines, 100M-row Zipf s101, analytic = D8 load:

- **Ratio executed/analytic: median 1.0000, range 1.0000–1.0001
  (n = 7).** The three J pipelines exceed the analytic load by exactly
  the +154 dim-side records the audit identified.
- **Spearman ρ = 0.953; Pearson on log10 loads ≈ 1.0000 (n = 7).**
  ρ < 1 despite near-identical loads because the analytic loads
  contain ties (G_A = J_A = W_A) that the executed J loads break by
  +154 records — a tie artefact, stated plainly.
- Executed load vs principal-stage max task duration: Spearman ρ =
  0.396 (n = 7, descriptive only).
- Executed load vs spill: **not interpretable** — exactly one of 7
  pipelines spilled (J_N); no association is claimed.

| Pipe | Stage | Analytic (D8) | Executed | Ratio | Shuffle read B | Dur max ms | Dur med ms | Max/Med | Spill B (tasks) |
|---|---|---|---|---|---|---|---|---|---|
| G_A | 6 | 10,873,845 | 10,873,845 | 1.0000 | 2,504,843,135 | 15,331 | 2,908.0 | 5.27 | 0 (0) |
| G_B | 11 | 1,571,791 | 1,571,791 | 1.0000 | 2,520,215,300 | 3,129 | 1,614.5 | 1.94 | 0 (0) |
| G_N | 16 | 10,442,419 | 10,442,419 | 1.0000 | 2,522,277,059 | 10,150 | 1,149.0 | 8.83 | 0 (0) |
| J_A | 22 | 10,873,845 | 10,873,999 | 1.0000 | 2,505,031,388 | 18,353 | 2,083.5 | 8.81 | 0 (0) |
| J_B | 30 | 1,571,791 | 1,571,945 | 1.0001 | 2,520,403,553 | 4,220 | 2,264.0 | 1.86 | 0 (0) |
| J_N | 38 | 10,442,419 | 10,442,573 | 1.0000 | 2,522,465,312 | 18,709 | 1,673.5 | 11.18 | 748,309,212 (1) |
| W_A | 51 | 10,873,845 | 10,873,845 | 1.0000 | 237,497,467 | 884 | 220.0 | 4.02 | 0 (0) |

Analytic F (separate): F_A = 54,690,910,911,173; F_B = 371,507,261,637
— analytic by construction (D4), never pooled.

## 4. Spill events — re-derived from the logs

Re-derivation **matches** the verifier/audit records exactly, byte for
byte, in both campaigns:

| Campaign | Pipeline (attribution) | Stage | Tasks | Bytes (log-derived) | Known value |
|---|---|---|---|---|---|
| Synthetic | J_N (stage is J_N's verifier-recorded principal stage) | 38 (collect) | 1 | 748,309,212 | 748,309,212 ✓ |
| Real-data Stage S | J_A (stage = S J_A principal stage; verifier per-entry spill total matches) | 298 (collect) | 2 | 2,517,426,184 | 2,517,426,184 ✓ |
| Real-data Stage S | J_B (stage = S J_B principal stage; verifier per-entry spill total matches) | 308 (collect) | 2 | 1,892,575,254 | 1,892,575,254 ✓ |

Real-data total: **4 tasks, 4,410,001,438 B** — the known value.
No other stage in either log spilled (305 real-data stages, 47
synthetic stages checked).

**Operational consequences.** All spilled tasks succeeded; both logs
contain 0 failed tasks in total (5,036 and 25,377 task ends). The
synthetic J_N pipeline's wall time (22.03 s, verifier-derived) is in
line with the non-spilling J_A (21.86 s): no job-level duration
penalty is visible. Per-task identification of the spilled task's own
duration is not retained in our stage aggregates, so no per-task
duration effect is claimed. Stage S J_A/J_B remained FAIL under both
thresholds on their executed loads (verifier) — a threshold outcome,
not an execution failure.

Retained verbatim, as frozen in the protocol: **no resource-induced
job non-completion was observed in either campaign**; the observed
failures (missing event-log S3 prefix; ApplicationMaster 100 s startup
timeout; three bootstrap-dead clusters) are infrastructure failures,
not data-induced failures.

## 5. Figure and data

- Figure: `figures/e4_analytic_vs_executed.pdf` / `.png` — log–log
  scatter, analytic vs executed load; physical points filled circles,
  analytic F points open squares (on the diagonal by construction,
  labelled as such), campaigns distinguished by colour, the one
  spill-affected point in the plotted sets (synthetic J_N) annotated.
  Stage S points are not in the frozen figure sets (Stage C + synthetic
  only); Stage S spill is reported in §4 above.
- Underlying data: `results/e4_figure_data.csv` (39 points),
  `results/e4_analysis_detail.json`, per-stage aggregates
  `results/e4_stage_aggregates_{synthetic,realdata}.json`,
  machine-readable rows `results/rows_e4.csv` (267 rows).

## 6. Caveats and NOT DERIVABLE items

1. Stage C executed-vs-spill association: NOT DERIVABLE — zero spill
   in Stage C (no variance). Synthetic executed-vs-spill association:
   not interpretable (1 of 7 pipelines spilled); none claimed.
2. Synthetic analytic load is the verifier's D8 in-job load (same
   partitioner as execution); the synthetic set therefore validates
   execution-vs-partition-model consistency, and the independent
   calibration evidence is the real-data Stage C set (§2).
3. D_A pipelines are excluded from all sets per §4 (diagnostic
   distinct-count, not a shuffle-read load).
4. Stage S (100M real-data) is outside the frozen E4 analysis sets;
   it is used only for the spill re-derivation (§4) — no Stage S
   calibration statistics are pooled into §2.
5. All pipeline-level statistics are descriptive over correlated
   instances (§7); the dataset-level n is 3 months (Stage C) and 1
   dataset (synthetic). No interval or equivalence claim is made.
6. Peak execution memory per pipeline is not in the event logs in a
   per-pipeline form and was NOT DERIVED.
