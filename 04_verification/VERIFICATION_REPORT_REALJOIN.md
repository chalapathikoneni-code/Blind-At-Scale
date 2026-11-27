# VERIFICATION REPORT — Real (Materialized) Self-Join Return (2026-11-26)

Capture: `EM_Results_RealJoin_20261126-071026Z.zip` (Drive EM_Results
id `1ENTW1Hzm2GlAaIgyKQdbRmwj6140N0X7`, 515,763 B, md5
`08d1488034ea07fce569d1e8a0c87422` — download read-back matches the
Drive metadata exactly). Local tree: `emr_results_realjoin/capture/`.
All numbers below were recomputed from the raw result JSONs and the
Spark event logs, never from the capture's README prose (the README's
numbers were checked against the raw artifacts; they agree except
where flagged in §5).

Both scales are present in the capture. The 4-part scale ran FIRST,
as briefed. Both runs executed on cluster **<CLUSTER_ID>**,
Spark **3.5.8-amzn-0**, with the frozen configuration confirmed from
the event logs' own Spark properties (not from prose):
`spark.sql.adaptive.enabled=false` (AQE off),
`spark.sql.shuffle.partitions=128`, `spark.executor.instances=18`,
**18 executors on 18 distinct hosts**, `spark.yarn.maxAppAttempts=1`,
dynamic allocation off. The guard was overridden for these two runs
only (`guard_overridden: true` in both pre-run JSONs, would-trip
reasons citing the frozen predictions against the 2,000,000,000
output cap and the 25,000,000 max-load cap), and the predictions were
recorded before execution: each run's `result_pre` JSON carries the
frozen prediction, and the event-log stage order corroborates it —
the stage that writes `result_pre.json` (stage 5) completes before
the join stages (6–7) begin.

## Was it the real join? — YES, with one strategy deviation (§4)

The as-run script `e1_selfjoin_real.py` executes an actual DataFrame
self-join — `left.join(right, l.author_id == r.author_id, "inner")`
projected to (work_id_left, work_id_right, author_id) — and writes
the joined rows to S3 Parquet
(`s3://YOUR_BUCKET/Access/RealJoin/<scale>/joined.parquet`); the output
row count in the result JSONs is a read-back count of that written
dataset. The event logs' physical plan confirms the execution shape:
`WriteFiles` over a Project over a shuffle join on `author_id` fed by
`Exchange hashpartitioning(author_id, 128)`. No
`count_per_partition`, no `glom`, and no counting shortcut is used
for the join itself (both names occur in the script only inside its
docstring, negated). The only counting in the script is (a) the exact
per-key `groupBy` counts that feed the frozen prediction — the same
prediction path as Nov 25 — and (b) the post-hoc read-back count of
the materialized output.

One structural note so the stage table is not misread: there is a
single exchange stage (stage 6). Its shuffle write is the left-row
count exactly once (5,368,834 / 1,260,852 records), while the join
stage's shuffle read is exactly twice that — Spark reused one
exchange for both sides of the self-join and each join task fetched
it for both the build and the probe side. Both sides were shuffled
by `author_id` into 128 partitions; nothing was broadcast
(`autoBroadcastJoinThreshold=-1` was set in the script, and no
BroadcastHashJoin appears in either plan).

## Run 1 — 4-part self-join (app <APPLICATION_ID>)

Step <STEP_ID>, **exit code 0** (controller log; step
process 182 s). Data: `Access/Final` (left rows 5,368,834). Event
log md5 `158d01c25f4613ab271dab2e11f8dc80`; result JSON md5
`da7c61de37510a04f5d8099793b5ec93`.

- **Status: OK — the real join COMPLETED and materialized.**
  Event log: 8/8 jobs JobSucceeded, 12 stages completed, **469
  tasks, 0 failed**, spill **0 B** memory / 0 B disk; application
  duration 162.2 s (the result JSON's `elapsed_sec` 112.1 s is the
  join+write action only, per the as-run script).
- Join+write stage (stage 7, 128 tasks), recomputed per-task from
  the event log: output records **max 123,158,197**, **median
  1,519,986.5**, **sum 795,242,814**; output bytes 3,477,379,546.
  The sum equals the result JSON's read-back count of the written
  Parquet exactly.
- Join exchange (stage 6, 44 tasks): shuffle write **5,368,834
  records / 109,310,564 B**; join-stage shuffle read **10,737,668
  records / 218,621,128 B** (per-task max 103,922, median 83,065).
- **Prediction vs measured:** total **795,242,814 vs 795,242,814 —
  exact** (difference 0). Max **123,158,197 vs predicted
  122,469,088 → Q (predicted/measured) = 0.994405**. **Verdict
  FAIL/FAIL as measured**: max exceeds the absolute limit
  (0.02N = 15,904,856.28) by **7.74×** and the relative limit
  (2× mean = 12,425,668.97) by **9.91×**.

## Run 2 — 1-part self-join (app <APPLICATION_ID>)

Step <STEP_ID>, **exit code 0** (controller log; step
process 130 s). Data: `Access/Final_1part` (left rows 1,260,852).
Event log md5 `04f69679b5721b8d9f0b4b8613eea7e6`; result JSON md5
`386223a28e007e84ce704ce43b43062e`.

- **Status: OK — the real join COMPLETED and materialized.**
  Event log: 8/8 jobs JobSucceeded, 12 stages completed, **390
  tasks, 0 failed**, spill **0 B** memory / 0 B disk; application
  duration 112.3 s (result-JSON write-action elapsed 69.2 s).
- Join+write stage (stage 7, 128 tasks): output records **max
  43,340,706**, **median 217,340**, **sum 262,464,138**; output
  bytes 438,037,502. Sum equals the read-back count exactly.
- Join exchange (stage 6, 13 tasks): shuffle write **1,260,852
  records / 25,134,303 B**; join-stage shuffle read **2,521,704
  records / 50,268,606 B** (per-task max 30,700, median 18,925).
- **Prediction vs measured:** total **262,464,138 vs 262,464,138 —
  exact** (difference 0). Max **43,340,706 vs predicted 43,344,257
  → Q = 1.000082**. **Verdict FAIL/FAIL as measured**: max exceeds
  the absolute limit (0.02N = 5,249,282.76) by **8.26×** and the
  relative limit (2× mean = 4,101,002.16) by **10.57×**.

## Prediction-vs-measured summary

| Scale | Predicted total | Measured total | Predicted max | Measured max | Q (max) | Verdict |
|---|---|---|---|---|---|---|
| 1-part | 262,464,138 | 262,464,138 (exact) | 43,344,257 | 43,340,706 | 1.000082 | FAIL/FAIL |
| 4-part | 795,242,814 | 795,242,814 (exact) | 122,469,088 | 123,158,197 | 0.994405 | FAIL/FAIL |

Cross-validation worth recording: the materialized join's
per-partition output distribution — max **and** median at both
scales — is identical to the Nov 25 streaming counter's distribution
(43,340,706 / 217,340 and 123,158,197 / 1,519,986.5). Two independent
execution paths (counting measurement vs executed join) produce the
same 128-partition load distribution, and both match the frozen
prediction to Q ≈ 1.0001 / 0.9944.

## §4 — Deviation: join strategy was ShuffledHashJoin, not sort-merge

The brief asked for the default sort-merge join, and the capture's
own result JSONs and README describe the join as "sort-merge". The
executed physical plan in both event logs is in fact
**`ShuffledHashJoin (HybridHash) [author_id], [author_id], Inner,
BuildRight`** — no SortMergeJoin appears in either log. The script
set `autoBroadcastJoinThreshold=-1` (effective: no broadcast) but
did not force sort-merge, and this EMR Spark build chose
ShuffledHashJoin. Consequences, stated exactly:

- The deviation does **not** touch the load comparison: both
  strategies consume the same `hashpartitioning(author_id, 128)`
  exchange, and the measured per-partition output distribution is
  the quantity the frozen prediction predicts. All numbers above
  stand.
- But any text describing these runs must not call them sort-merge.
  The capture's `frozen_config.join` field ("sort-merge,
  autoBroadcastJoinThreshold=-1") is inaccurate as a description of
  what executed and should not be quoted.

## §5 — README cross-check

The capture README's figures agree with the recomputation above in
every particular tested, with three exceptions: (i) the
"sort-merge" label (§4); (ii) its "wall-clock 112.1 s / 69.2 s" are
the write-action elapsed times from the result JSONs — full
application durations are 162.2 s / 112.3 s; (iii) the 4-part median
is printed as 1,519,986 vs the exact 1,519,986.5 (the capture's own
metrics JSON carries the exact value, and the recomputation matches
the metrics JSON on every stage figure).

## Scripts — received; diff performed

- **common.py — byte-identical** to the Nov 25 patched script
  (md5 `c5cd8d7f09fbfa7bde4daeae14b13af3`, diff empty).
  `predict_from_counts` and `verdict` are therefore unchanged; the
  predictions in the pre-run JSONs equal the frozen values exactly
  at both scales.
- **e1_selfjoin_real.py — new script** (no Nov 25 counterpart).
  Relative to the Nov 25 self-join path it changes exactly the
  execution: (a) the counting measurement is replaced by the real
  join + Parquet materialization + read-back count; (b) the
  prediction block, guard caps, `--force-measure` override
  recording, and pre-execution `result_pre.json` write are carried
  over unchanged in structure; (c) `autoBroadcastJoinThreshold=-1`
  is set in-session. Prediction math is untouched. The as-run code
  for these runs IS included in the capture — the as-run gap that
  remains open for the November runs (C25) does not extend to this
  run.

## What this run establishes — and what it does not

**Establishes.** The OpenAlex author self-join was executed as a
real, materialized join under the frozen configuration at both
scales, on the record, with predictions registered before
execution. It completed at both scales with zero failed tasks and
zero spill, and its measured per-partition load distribution
matches the frozen prediction (totals exact; hottest partition
Q 1.000082 at 1 part, 0.994405 at 4 parts) with verdict FAIL/FAIL
at both scales. The hostile-review objection that the self-join
evidence was "a load measurement, not a normal materialised join"
is answered for these runs: the FAIL/FAIL verdict now rests on an
executed, materialized join as well as on the counting measurement,
and the two agree exactly.

**Does not establish.** (i) Nothing about sort-merge execution —
the strategy was ShuffledHashJoin (§4). (ii) Completion is not in
tension with the FAIL verdict and must not be presented as
softening it: the verdict is a statement about hottest-partition
load against the frozen limits (7.7–10.6× over), and a job can
complete while carrying exactly the load concentration the paper
flags — here the hottest of 128 tasks produced 123,158,197 of
795,242,814 rows. (iii) No new arm/concealment evidence: the
concealment component of the decision rule was closed by the Nov 25
counter runs; this run tests the synopsis prediction against a
materialized execution only. (iv) C25 (November as-run code) is
unchanged and stays open.
