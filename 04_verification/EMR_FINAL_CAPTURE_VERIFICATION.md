# VERIFICATION REPORT — Blind at Scale FINAL campaign EMR return (Nov 25)

Verifier: Muse (parent-directed subagent), 2026-11-25. Pattern mirrors
`emr_results_synthetic/VERIFICATION_REPORT.md` and
`emr_results_realdata/VERIFICATION_REPORT.md`: every number below was
recomputed from the raw artifacts in the capture, not taken from step
summaries or console screenshots. Screenshot/Kiro claims were treated
as claims and audited (§7).

## 0. Capture identity

- Drive file: `bas_final_capture_20261125-192309Z.zip`, EM_Results
  (`1P3X1PAQXMSWpEvBr73QNTC2aue4HksLv`), file id
  `1WqQwhXhJGyvSxV7D9-oyghhbYkbz-U-Y`.
- Size 2,179,326 B; md5 `43b51cf904494b5cd89a2369c48270a4` — Drive
  checksum = downloaded file checksum. **Gate: PASS.**
- Unzipped to `~/workspace/blind-at-scale/emr_results_final_capture/`:
  75 files (10 output JSONs + 1 CSV dir, 5 Spark event logs, 17 step-log
  directories, MANIFEST.txt). Capture is a point-in-time snapshot taken
  2026-11-25 19:23:09 UTC while Step 6 was still running (MANIFEST.txt
  states this; independently confirmed in §5).

## 1. Inventory — present vs expected

PRESENT AND VERIFIED: smoke (Step 0), prep (Step 1, second attempt),
E1 join_dim at 4 parts (Step 2) and at 8 parts (Step 6, partial),
E6 join_dim off+on (Steps 3/4), A9 at 100M (Step 5), E6 synthetic
J_A/J_N off+on (Steps 7a/7b), step logs for all of the above plus the
November steps on both clusters.

EXPECTED-MISSING (arrive with the Step 6 + recovery return): Step 6
outputs beyond join_dim_s8 (selfjoin at s8/s12, join_meta, s12 cases,
works×sources family); Step 6's event log; recovery R1–R4 outputs
(patched guard records incl. the exact selfjoin Σcount², the 1-part
measured selfjoin, E1 join_meta, the E6 1-part selfjoin AQE pair).

FINDINGS — missing that was NOT expected-missing:

1. **Event logs for the three FAILED cluster-A applications are absent
   from the capture** (app _0005 E1, _0006 E6-off, _0007 E6-on; Step 6's
   application log also absent, expected while running). The capture's
   `eventlog/` holds only cluster-A smoke (_0003) + prep (_0004) and
   the three cluster-B applications (_0003/_0004/_0005). Consequence:
   completed-stage shuffle/spill volumes for the selfjoin attempts
   cannot be recomputed from this capture; the failure evidence in §6
   rests on the step logs, which are complete. If these logs still
   exist in S3 (`Access/Final/eventlog/` or the cluster log path),
   they should be pulled with the next return.
2. **The S3 retrieval items (REPRODUCIBILITY_CLOSURE_REPORT §1) are NOT
   in the capture** — no November as-run scripts, no counts.npz. M9
   archival half stays OPEN (§8).
3. **Kiro's patched scripts are NOT in the capture** (recovery not yet
   run), so the patch diff (streaming counting / guard extension /
   prediction math unchanged) cannot be performed yet. PENDING.

## 2. Placement + execution gates (event-log derived)

All five captured applications ran on Spark 3.5.8-amzn-0, user hadoop,
**18 executors on 18 distinct hosts** (placement gate ≥ 2 hosts: PASS
for every captured app), 0 failed tasks in every completed app:

| App | Name | Cluster | Wall (UTC) | Tasks | Failed | Shuffle R/W (B) | Spill |
|---|---|---|---|---|---|---|---|
| _1795448833601_0003 | bas-e1e6-smoke | A j-07648841 | 18:08:37–18:09:12 (34.2 s) | 22 | 0 | 211,749 / 211,749 | 0 |
| _1795448833601_0004 | bas-e1e6-prep | A | 18:17:55–18:18:58 (62.8 s) | 954 | 0 | 365,777,370 / 365,777,370 | 0 |
| _1795491295161_0003 | bas-e1e6-a9-100m | B j-02689641 | 19:01:43–19:02:37 (53.8 s) | 1,245 | 0 | 71,523,787 / 71,523,787 | 0 |
| _1795491295161_0004 | bas-e1e6-e6synth-off | B | 19:02:48–19:04:56 (128.1 s) | 548 | 0 | 5,039,703,273 / 5,039,703,273 | 0 |
| _1795491295161_0005 | bas-e1e6-e6synth-on | B | 19:05:07–19:07:14 (127.5 s) | 548 | 0 | 5,039,703,273 / 5,039,703,273 | 0 |

Cross-checks inside the logs: prep stage outputs reproduce
`prep_summary.json` exactly (fact 5,368,834 rows out; dim 3,064,777
rows out). A9 stage inputs: 4 scans of 100,000,000 (fact kA/kB/kC/kN)
+ 2 scans of 60,000,000 (fact2 kA/kB) = 520,000,000 records — the Nov
corpus shape (fact 100M / fact2 60M / 20k keys; map-side pair counts
1,438,521–1,440,000 = 72 partitions × 20,000 keys, consistent).
E6-synth join stages read 101,438,521 = 100,000,000 + 1,438,521 records
(J_A) and 101,438,220 = 100,000,000 + 1,438,220 (J_N) — the same
distinct-key pair counts the A9 app produced independently
(1,438,521 / 1,438,220). Two applications, one corpus, exact agreement.

## 3. E1 — OpenAlex join validation (Steps 1, 2, 6-partial)

Staging: OpenAlex snapshot 2026-10-14, 4 largest works parts,
4,232,357,580 B staged, 1,539,526 works → fact_authorships 5,368,834
rows, dim_authors 3,064,777 rows. Top author count 11,056 (natural
skew; the self-join candidate). **First prep attempt FAILED**
(step <STEP_ID>, ~18:11 UTC): the in-job
`aws s3 cp --no-sign-request` s3→s3 staging copy exited 1 before any
Spark application started (no event log exists for it — consistent).
Classification: infrastructure (staging), recovered by the re-run
(<STEP_ID>, app _0004), which staged via local
download/upload and completed with ret 0. The failure is recorded
here because the run's history is part of the evidence; it touches no
result.

**join_dim (fact ⋈ dim on author_id), 4 parts — VERIFIED.**
Measured (job's glom count; JSON internally consistent: 128 loads sum
to output rows exactly; max/median recompute exactly; verdicts
recompute under the frozen convention rel 2.0×mean / abs 0.02N):
output 5,368,834 rows, max partition load 51,961, median 41,532.5,
mean 41,944.02 → **PASS / PASS** (the frozen PASS control behaves as a
control). Arms vs measured max (Q = symmetric ratio):
synopsis pred 50,856, Q 1.0217, PASS/PASS ✓; endbiased pred 54,656,
Q 1.0519, PASS/PASS ✓; **uniform pred 120,000, Q 2.3094, FAIL/FAIL —
a FALSE POSITIVE**: the 1% uniform arm invents a failure on a
workload whose executed load passes with 38% headroom under the abs
limit. Synopsis predicted total is exact (5,368,834; fan-out 1);
its max is not exact (50,856 vs 51,961) because the prediction assigns
keys by crc32 while Spark hash-partitions — the documented
approximation in `common.py`, direction: 2.2% underprediction of max.

**join_dim at 8 parts (Step 6, partial) — INTERNALLY CONSISTENT,
event-log cross-check PENDING.** Step 6 staged 12 parts total and
completed join_dim_s8 before the capture: left rows 10,218,360,
measured max 89,773, median 79,449.0 → PASS/PASS. Arms: synopsis
92,147 (Q 1.0264 ✓), endbiased 92,811 (Q 1.0338 ✓), uniform 510,000
(Q 5.6810, FAIL/FAIL — false positive again, worse with scale).
JSON sums/max/median/verdicts all recompute; the application's event
log arrives with the Step 6 return, so this case is one verification
tier below join_dim-4 until then.

**selfjoin (fact ⋈ fact on author_id) — OBSERVED EXECUTION FAILURE;
predicted verdict FAIL/FAIL by a wide, proven margin.** The case ran
in Steps 2, 3 and 4 and killed all three applications (§6). No
metrics JSON exists (the job died inside the counting action before
writing). What is provable from the capture alone:
- Single-key output: the largest author (11,056 works) contributes
  11,056² = **122,235,136 output rows, all landing in ONE partition**
  under the key partitioning. Max partition load ≥ 122,235,136.
- Σcount² (total output) ≥ **624,940,052** — the top-30 author counts
  in `prep_summary.json` alone square-sum to this; the full vector
  only adds.
- At that proven lower bound, the frozen limits are abs 12,498,801 /
  rel 9,764,688: the single-partition load exceeds them by **9.8× /
  12.5×**. The exact Σcount² and predicted max are pinned by the
  recovery guard record (deterministic from the count vector).
- Input-side comparison (frozen decision rule's ratio): predicted
  joined max ≥ 122,235,136 vs the larger input-side max load 51,961
  (join_dim's measured fact-side partition max, identical partitioning)
  = **≥ 2,352×**, vs the rule's 1.5× requirement.
- Reduction-arm concealment for the selfjoin: NOT EVALUABLE from this
  capture (the arms' selfjoin predictions were computed in the crashed
  application and never persisted). Pending recovery.

**join_meta (work_id control)** — never ran: `e1_join.py`'s case order
is join_dim → selfjoin → join_meta, so the selfjoin crash preempted
it. Recovery scope (R1). Not a finding against the run; a consequence
of the crash position.

## 4. E6 — AQE robustness (Steps 3, 4, 7a, 7b)

**join_dim pair — VERIFIED, AQE changed nothing.** Off (app _0006):
max 51,961, elapsed 11.1 s, plan flags AQEShuffleRead False /
skew-join False. On (app _0007): max 51,961, elapsed 11.2 s, flags
False / False. The 128 per-partition loads are **bit-identical**
across conditions and identical to Step 2's independent measurement.
(Caveat recorded: the on-run's plan text is the initial adaptive plan,
`isFinalPlan=false` — a weak instrument alone; the decisive evidence
is the physical identity of the executed loads.)

**Synthetic J_A / J_N pair — VERIFIED from event logs, AQE did not
engage.** Off (app _0004) vs On (app _0005), both 18 hosts, 548 tasks,
0 failed, spill 0 B in both conditions:
- J_A: max load 10,873,845 (both), median 528,284.5 (both), output
  100,000,000 (both), verdict FAIL/FAIL (both); elapsed 47.1 s off /
  47.7 s on. Partition-load arrays identical.
- J_N: max load 10,442,419 (both), median 506,570.5 (both), output
  100,000,000 (both), verdict FAIL/FAIL (both); elapsed 43.3 s off /
  41.6 s on. Arrays identical.
- Event-log totals identical **to the byte**: shuffle read = shuffle
  write = 5,039,703,273 B / 202,876,741 records in both applications.
  AQE produced no coalescing, no skew split, no metric change of any
  kind on the two predicted-hot synthetic cases. (Today's J_N spilled
  0 B vs 748,309,212 B in the November pipeline run — different job
  shape (single join + count vs the full pipeline); not a
  contradiction, and no cross-job spill claim is made.)
- Cross-run consistency: today's executed J_A/J_N output-partition
  maxima (10,873,845 / 10,442,419) sit exactly 154 records below the
  November GT shuffle-read loads (10,873,999 / 10,442,573) in both
  cases — the November figure is the join stage's total shuffle read
  (fact side + 154 dim-side records in the fullest task). Two
  independent runs, two measurement definitions, exact reconciliation.

**Selfjoin pair — both conditions crashed identically** (§6). AQE-on
did not rescue the case: skew-join optimisation is absent from the
on-run's recorded plan evidence, and the application died in the same
shuffle/materialisation distress as AQE-off. The paired 1-part
selfjoin AQE comparison is recovery scope (R3).

Frozen-interpretation outcome for the available scope: AQE did not
engage on any completed case (plan evidence + physical identity), and
the predicted-hot cases' FAIL loads persist bit-identical under
AQE-ON → on this evidence the predicted pressure is **not**
engine-mitigated by AQE on this engine/configuration, and the
prediction's operational meaning stands. The selfjoin pair's
completion would extend, not change, the available-scope statement
unless it contradicts it.

## 5. Step 6 state at capture

Step <STEP_ID> was mid-run: 12 parts staged into
`raw_works`, join_dim_s8 written at 19:22 UTC (stdout), stderr
snapshot ends 19:22:07 UTC with normal Spark INFO traffic, no
exit/ret line. Remaining Step 6 cases (selfjoin s8/s12, join_meta,
s12, works×sources) and its event log are PENDING USER RETURN, as the
capture's own MANIFEST states.

## 6. Failure evidence — the three crashed applications

All three failures share one shape: join_dim completes and writes its
JSON; the selfjoin's shuffle + in-memory partition counting
(`joined.rdd.glom().map(len).collect()`) then drives executors into
distress until the SparkContext shuts down and the driver dies with
`Py4JJavaError … collectAndServe` / "Job cancelled because SparkContext
was shut down", step exit ret 1. **Classification (all three):
observed execution failure, resource/materialisation class** — the
join's output volume (single-partition ≥ 1.22×10⁸ rows; total
≥ 6.25×10⁸ rows) saturated node disk/network and executor memory
until the cluster killed executors. NOT infrastructure failure: both
clusters remained healthy and completed other steps before and after.
NOT a threshold violation: the jobs did not finish and report a
verdict; they failed. (The frozen verdict the load *would* have
received is FAIL/FAIL by the §3 bounds — stated as a prediction,
never as the failure's substitute.)

| Step / app | Window (UTC) | Signature (step-log derived) |
|---|---|---|
| Step 2 E1, app _0005 | 18:20:13–18:28:31 (8m18s) | Selfjoin counting stage 30.0: first executor lost 18:25:41 (executor 5, "Executor Process Lost"), executor 17 lost 18:26:21; 12 lost-executor events, exit code 143 ×11, heartbeat ×38; no DataStreamer distress (fast collapse); root "Job 16 cancelled because SparkContext was shut down" |
| Step 3 E6-off, app _0006 | 18:28:42–18:54:13 (25m31s) | DataStreamer exceptions from 18:29:57 (×21, datanode "is bad" ×3); executor 2 killed 18:36:01 "Container from a bad node", exit 143, in stage 7.0; FetchFailed ×99; heartbeat ×50; root "Job 4 cancelled because SparkContext was shut down" |
| Step 4 E6-on, app _0007 | 18:54:24–19:14:11 (19m47s) | DataStreamer exceptions from 18:55:43 (×58, "is bad" ×8); executor 6 killed 19:05:49 "Container from a bad node", exit 143, in stage 10.0; heartbeat removals with gaps 122,567–174,504 ms; FetchFailed ×11; root "Job 7 cancelled because SparkContext was shut down" |

Because the failed applications' event logs are absent (Finding 1),
per-stage shuffle/spill volumes for the selfjoin attempts cannot be
recomputed; the table above is the complete evidence this capture
supports, and it is sufficient for the classification.

## 7. Audit of screenshot / Kiro claims against the raw artifacts

- "join_dim off max load 51961, skew-join in plan: False" — **EXACT**
  (JSON + plan text).
- "join_dim on max load 51961, skew-join: False" — **EXACT**.
- "top author has 11,056 works → ~122M rows from that one key" —
  **EXACT** (11,056 in prep_summary; 11,056² = 122,235,136).
- "Step 4: executor 6 killed, exit 143; selfjoin is stage 10" —
  **EXACT** (19:05:49 UTC, stage 10.0 lost tasks).
- "heartbeat timeouts 150–175 s" — **SUBSTANTIATED in range**:
  observed heartbeat gaps 122,567–174,504 ms (the specific executor
  list 3/13/16/18 was not exhaustively re-verified).
- "~15 min of DataStreamer timeouts" (Step 4) — **approximately
  right**; log-derived distress window 18:55:43→19:14:11 ≈ 18.5 min.
- "Step 6 still RUNNING; synthetic cluster healthy" — **CONFIRMED**
  (§5; cluster B completed all three of its steps).
- **DISCREPANCY — Kiro's guard diagnosis is wrong on one point.**
  Kiro: the guard "uses the synopsis arm's predicted total_rows, and
  that prediction doesn't model self-join fan-out (Σ count²), so it
  under-predicted, stayed under the 2B cap." The delivered code
  contradicts the middle clause: `common.py`
  `partition_loads_from_counts` with fanout=None sets f = c and
  out = c × f = c² per key — the synopsis prediction **does** include
  Σcount² (proven ≥ 624,940,052 from the captured top-30 alone). The
  guard stayed silent because the predicted TOTAL (≈6.25×10⁸) is
  below the 2×10⁹ cap: the cap watches total rows, while the lethal
  quantity is the single-partition concentration (≥1.22×10⁸ rows in
  one partition) plus the shuffle volume. The recovery patch (max-
  partition guard) targets exactly this. The parent agent's correction
  to the author stands as the accurate account; Kiro's failure
  mechanism (glom materialisation + spill saturation) is confirmed
  by the logs.

## 8. Reproducibility / M9 status after this capture

- The fast-lane campaign's own code is archived (delivered package,
  local + Drive E1_E6_FINAL) and the as-run step commands are in the
  captured controller logs (script names + confs extracted for every
  step; AQE confs verified: adaptive.enabled=false for Steps 2/3/7a,
  true for Steps 4/7b — the ONLY conf difference within each pair).
- November retrieval (§1 of REPRODUCIBILITY_CLOSURE_REPORT): **NOT in
  this capture. M9 archival half remains OPEN.** No as-run November
  scripts, no counts.npz were delivered; nothing was diffed; nothing
  is claimed.
- Patched recovery code: not yet in existence in the return path;
  the (a)/(b)/(c) patch audit is PENDING the recovery return.

## 9. What remains open after this verification

Step 6 completion (selfjoin s8/s12 guard-or-fail records, join_meta,
s12, works×sources, its event log) and the recovery batch R1–R4
(exact selfjoin Σcount² + guard record, 1-part measured selfjoin,
E1 join_meta, E6 1-part selfjoin AQE pair, patched-code audit), plus
the November retrieval (M9). The E1 frozen decision rule's
concealment half and the selfjoin's measured max load are the two
quantities this capture cannot supply; every other E1/E6/A9 quantity
in the verdict's pending slots is now filled from verified artifacts
(see the updated E1/E6 reports and FINAL_SCIENTIFIC_VERDICT).
