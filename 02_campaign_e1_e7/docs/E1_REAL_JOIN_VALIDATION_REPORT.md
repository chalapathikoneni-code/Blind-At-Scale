# E1 — Real-World Join-Emergent Resource Pressure: FINAL FOR AVAILABLE SCOPE

**Status: EXECUTED on EMR 2026-11-25 (author's office AWS account) and
VERIFIED by Muse from the returned capture
(`bas_final_capture_20261125-192309Z.zip`, Drive EM_Results; full record:
`EMR_FINAL_CAPTURE_VERIFICATION.md`).** Filled slots: staging + prep,
join_dim at two scales, the selfjoin's predicted bounds and its
observed execution failure (×3 applications). **Still PENDING USER
RETURN:** Step 6's remaining cases (selfjoin s8/s12, join_meta, s12,
works×sources) and the recovery batch (exact selfjoin Σcount² guard
record, 1-part measured selfjoin, E1 join_meta). The frozen decision
rule's concealment half cannot be evaluated until the recovery return;
this report states exactly what is and is not established.

## 1. Dataset gate — as run

The fast lane ran **OpenAlex** (the gate analysis's documented
alternate: naturally occurring, multi-table with a non-trivial
dimension, staging-verifiable — consistent with the gate in
`FINAL_EXECUTION_PROTOCOL.md` §1; the MathOverflow recommendation
stands in that protocol as the cross-check record). Snapshot
2026-10-14, 4 largest works parts, 4,232,357,580 B staged,
1,539,526 works → **fact_authorships 5,368,834 rows**,
**dim_authors 3,064,777 rows**. Natural skew: top author 11,056 works;
top-30 counts 11,056 … 1,911. Cases (fast-lane package, frozen before
the run): `join_dim` (fact ⋈ dim on author_id, fan-out 1 — PASS
control), `selfjoin` (fact ⋈ fact on author_id — the join-emergent
candidate), `join_meta` (fact ⋈ works_meta on work_id — PASS control);
arms synopsis / endbiased (top-64 + 1% tail) / uniform (1%), then the
full join executes. Threshold convention (frozen, `common.py`):
P = 128; FAIL iff max partition load > 2.0 × mean (rel) or > 0.02 × N
(abs), reported side by side.

Staging history (part of the record): the first prep attempt failed
at the in-job s3→s3 copy (infrastructure, before any Spark app
started); the re-run staged via local download/upload and completed.
Prep application verified from its event log: 18 executors / 18
distinct hosts, 954 tasks, 0 failed; stage outputs reproduce the
summary's row counts exactly.

## 2. Verified results

### join_dim, 4 parts (Step 2) — PASS control behaves as a control
Measured: output 5,368,834 rows; max partition load **51,961**;
median 41,532.5; mean 41,944.02 → **PASS / PASS** (38% headroom under
the abs limit). JSON fully recomputed (128 loads sum exactly; max /
median / verdicts exact). Arms vs measured max (Q = symmetric ratio):
- synopsis: pred max 50,856, **Q 1.0217**, verdict PASS/PASS ✓
  (predicted total exact at fan-out 1; max differs by the documented
  crc32-vs-Spark hash-partitioning approximation, −2.2%).
- endbiased: pred max 54,656, **Q 1.0519**, verdict PASS/PASS ✓.
- uniform: pred max 120,000, **Q 2.3094**, verdict FAIL/FAIL —
  **FALSE POSITIVE**. The 1% uniform arm invents a failure on a
  workload that passes executed. (Direction matters: this is a false
  alarm, not a concealment.)

### join_dim, 8 parts (Step 6, partial) — internally consistent, event-log check pending
Measured: left rows 10,218,360; max **89,773**; median 79,449.0 →
PASS/PASS. Arms: synopsis Q 1.0264 ✓, endbiased Q 1.0338 ✓, uniform
pred 510,000, **Q 5.6810, FAIL/FAIL — false positive again, growing
with scale**. The JSON recomputes exactly, but Step 6's event log
arrives with its return, so this case sits one verification tier
below the 4-part case until then.

### selfjoin — observed execution failure; predicted FAIL/FAIL by a proven wide margin
The case ran in Steps 2, 3 and 4 and **killed all three applications**
(full signatures in the verification report §6): the shuffle +
in-memory partition counting drove executors into disk/network
saturation and loss cascades until each SparkContext shut down
(Step 2: 8m18s, fast collapse in stage 30; Step 3: 25m31s, DataStreamer
distress from minute 1; Step 4: 19m47s, executor 6 killed exit 143 in
stage 10). Classification: **observed execution failure,
resource/materialisation class** — not infrastructure (both clusters
stayed healthy and completed other steps), and not a threshold
violation (no verdict was ever produced by these runs).

Provable from the capture alone (count-vector arithmetic, no
modelling choices):
- The largest key alone contributes 11,056² = **122,235,136 output
  rows in a single partition** → max partition load ≥ 122,235,136.
- Total output Σcount² ≥ **624,940,052** (top-30 counts alone).
- At that lower bound the frozen limits are 12,498,801 (abs) /
  9,764,688 (rel): the single-partition load exceeds them by
  **9.8× / 12.5×** → predicted verdict **FAIL / FAIL**, robust to any
  completion of the tail. Exact Σcount² + predicted max: recovery
  guard record (deterministic from the count vector).
- Decision-rule ratio: predicted joined max ≥ 122,235,136 vs the
  larger input-side max load 51,961 (identical partitioning) =
  **≥ 2,352×**, against the rule's 1.5× requirement.

### join_meta — PENDING (recovery)
Never ran: `e1_join.py`'s case order (join_dim → selfjoin →
join_meta) put it behind the crash. Recovery R1 scope.

## 3. Frozen decision rule — evaluation state

Rule (frozen): join-emergent pressure on natural data is supported
iff ≥ 1 joined workload is GT FAIL with joined max load ≥ 1.5× the
larger input-side load, AND ≥ 1 reduction arm conceals it.

- Component 1 (a joined workload under genuine emergent pressure):
  the selfjoin's pressure is established in the strongest available
  form — three observed execution failures — and its predicted
  verdict is FAIL/FAIL with the ≥ 1.5× ratio exceeded by ≥ 2,352×
  on proven bounds. The rule's letter asks for a *measured* max load;
  the measurement is precisely what the failure prevented at this
  scale. The 1-part recovery run supplies a measured selfjoin at a
  feasible scale.
- Component 2 (concealment by a reduction arm): **NOT EVALUABLE**
  from this capture — the arms' selfjoin predictions were computed
  inside the crashed application and never persisted. (On the PASS
  control, the observed arm failure mode is the opposite: uniform
  false-alarms.) Pending the recovery guard record, which regenerates
  the predictions deterministically.

**Therefore C28 is NOT closed by this return.** It is substantially
narrowed: natural-data join-emergent resource pressure now has an
observed execution failure on OpenAlex plus a verified PASS control,
where before it rested on synthetic data alone. The formal rule
verdict waits on the recovery batch, and no wording may anticipate it.

## 4. Pending slots (explicit)

| Item | Arrives with |
|---|---|
| Exact selfjoin Σcount², predicted max, arm predictions + SKIPPED_GUARD record (4-part) | Recovery R1 |
| 1-part measured selfjoin (max load, spill, event log) | Recovery R2 |
| E1 join_meta (4-part) | Recovery R1 |
| Step 6 remainder: selfjoin s8/s12, join_meta, s12 cases, works×sources (second family), Step 6 event log (incl. join_dim_s8 cross-check) | Step 6 return |
| Failed applications' event logs (_0005/_0006/_0007), if still in S3 | Next return (Finding 1 of the verification report) |

---

## 5. FINAL RETURN UPDATE — 2026-11-26 (COMPLETE capture, verified)

Source: `emr_results_final_return/VERIFICATION_REPORT_FINAL_RETURN.md`
(capture md5 c126977ec0ad573a75288c6415deda4d; all numbers recomputed
from raw JSONs/event logs).

- **join_dim_s8: event-log VERIFIED.** Step 6 app
  (<APPLICATION_ID>) stage 18 shuffle records written =
  10,218,360 = the JSON's output rows; jobs 0–14 succeeded. The "one
  verification tier lower" flag is REMOVED.
- **Step 6 ended at the s8 self-join**: stage 30 collect, 40 failed
  task attempts, Job 15 failed. No selfjoin_s8/s12 outputs; s12 never
  ran. (Fourth observed self-join execution failure, in the unpatched
  Step 6 code path.)
- **join_meta (R1, 4-part): MEASURED.** Max partition load 43,348,
  PASS/PASS; synopsis Q 1.0037, end-biased Q 1.1059 (both agree);
  **uniform FALSE POSITIVE (Q 2.5376)** — third in E1.
- **Selfjoin guard records (exact predictions, replacing all bounds):**
  4-part Σcount² = **795,242,814**, predicted max load **122,469,088**
  (4.90× the 25M cap); 1-part Σcount² = 262,464,138, predicted max
  load 43,344,257 (1.73× cap). All arms FAIL/FAIL at both scales.
- **1-part selfjoin: SKIPPED_GUARD — NOT MEASURED.** The guard skipped
  it at 1 part as well. No executed self-join measurement exists.
- **works×sources (R4): join_source_dim MEASURED FAIL/FAIL**, max
  171,026; synopsis Q 1.0117, end-biased Q 1.0063 (both agree);
  uniform Q 2.4558 (verdict agrees). A measured join-emergent FAIL on
  natural data in a second family, called correctly by both HH arms.
  selfjoin_source SKIPPED_GUARD (both caps exceeded; Σcount²
  44,878,025,678).

**C28 after the final return: STILL NOT CLOSED (PARTIAL).** The frozen
rule requires a *measured* self-join; the guard record half is now
complete and exact, but the measured half does not exist — the guard
itself prevented the measurement at every scale. The concealment
component remains unevaluated for the self-join. Supported wording is
exactly: measured emergent FAIL in the second family with both HH
arms correct; self-join predicted FAIL on exact accounting at two
scales, observed to fail execution four times, guard-classified
beyond the pre-registered caps. No stronger statement is licensed.

---

## MEASURED SELF-JOIN — 2026-11-26 (author-ordered, verified)

Full record: `VERIFICATION_REPORT_SELFJOIN.md` in
`emr_results_selfjoin_measured/`. The author ordered the guard
overridden (`--force-measure`, predictions recorded first,
thresholds unchanged) and the self-join measured at both scales
with the patched streaming counter, on cluster
<CLUSTER_ID>, Spark 3.5.8-amzn-0, AQE off. Both jobs
COMPLETED (event-log verified: 18 executors / 18 distinct hosts,
0 failed tasks, 0 spill at both scales).

- **1 part** (app _0014; 453 tasks; 198 s): measured output rows
  **262,464,138 = predicted Σcount² exactly**; measured max
  partition load **43,340,706** vs predicted 43,344,257 →
  synopsis **Q 1.000082**; **FAIL/FAIL as measured** (8.26× the
  absolute limit, 10.57× the relative limit). Arm Qs vs measured:
  end-biased 1.024735, uniform 1.155726 — all FAIL/FAIL.
- **4 parts** (app _0015; 608 tasks; 387 s): measured output rows
  **795,242,814 = predicted Σcount² exactly**; measured max
  partition load **123,158,197** vs predicted 122,469,088 →
  synopsis **Q 0.994405**; **FAIL/FAIL as measured** (7.74× /
  9.91×). Arm Qs: end-biased 1.030343, uniform 0.727114
  (understates the max by 27%, verdict still FAIL).
- **Concealment component: EVALUATED — no arm conceals the load
  at either scale** (all three arms FAIL/FAIL against the measured
  ground truth).

**C28 after the measured return: CLOSED — VERIFIED.** Both
outstanding components of the frozen decision rule (measured max;
concealment) are now evaluated at two scales. The four earlier
observed execution failures remain in the record as the
pre-measurement history. The patched scripts were delivered with
this capture and diffed (see the verification report): streaming
counting, guard extension + recording, case selection, and the
force override — **prediction math unchanged (G03 closed)**.
