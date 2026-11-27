# Verification Report — Final Return (COMPLETE capture, 2026-11-26)

**Capture:** `bas_COMPLETE_capture_20261126-000433Z.zip`, Drive EM_Results id
`1UHYYCH6iwkmC9Se9aZ4VL9IAtg3NmW7s`, 6,797,920 B, md5
`c126977ec0ad573a75288c6415deda4d` — **download read-back md5 matches Drive
exactly**. Local tree: `~/workspace/blind-at-scale/emr_results_final_return/`.
This is the only new author return since the v2 build (Drive-wide
modified-since search, 2026-11-25 23:40Z: only this zip is Blind-at-Scale
material). All previously captured output JSONs re-included in this capture
are **byte-identical** to the Nov 25 capture (md5-checked file by file:
a9_100m, e1_join_dim, e1_join_dim_s8, e6 off/on join_dim + synth J_A/J_N,
prep_summary, smoke).

All numbers below were recomputed from the raw JSONs and event logs in this
capture. No summary prose was trusted.

## Item-by-item verdict against the pending list

### 1. Step 6 remainder — PARTIAL
- **join_dim_s8 event log: FOUND + VERIFIED.** Step 6's application
  (`<APPLICATION_ID>`, `bas-e1e6-e1-more`, cluster
  <CLUSTER_ID>) is in the capture (file suffix `.inprogress`, but
  the log is complete through ApplicationEnd). Stage 18 shuffle records
  written = **10,218,360**, exactly the measured output rows in
  `e1_join_dim_s8.json`; jobs 0–14 all JobSucceeded. The 8-part numbers
  (max 89,773; synopsis Q 1.0264; end-biased Q 1.0338; uniform false
  positive Q 5.6810) are now **event-log verified — the "one verification
  tier lower" flag is removed**.
- **Step 6 then FAILED at the s8 self-join**: stage 30 (collect) recorded
  40 failed task attempts, Job 15 JobFailed, application ended. Consistent
  with the unpatched counting path failing on the self-join, as in Steps
  2–4. **No selfjoin_s8/s12 outputs exist; the s12 cases never ran.**
  Selfjoin s8/s12 guard records: **ABSENT** (the run predates the guard
  patch; it crashed instead of skipping).
- join_meta: delivered via recovery R1 (below), not Step 6.
- works×sources: delivered via recovery R4 (below).

### 2. Recovery batch — RAN, verified from raw artifacts
All recovery applications ran on cluster <CLUSTER_ID>, each
**18 executor hosts, 0 failed tasks, 0 spill** (event logs
`<APPLICATION_ID>_1` … `_0013_1`):

| App | Step | Tasks | Note |
|---|---|---|---|
| _0006_1 | R1 e1 (join_meta + selfjoin) | 1,813 | join_meta measured; selfjoin SKIPPED_GUARD |
| _0007_1 | R2a 1-part prep | 948 | fact 1,260,852 rows; staged 1,070,341,481 B |
| _0008_1 | R2b 1-part selfjoin | 312 | SKIPPED_GUARD (see below) |
| _0009_1 | R3a e6-off join_meta | 731 | shuffle write 257,923,472 B |
| _0010_1 | R3b e6-on join_meta | 603 | shuffle write **257,923,472 B — byte-identical to off** |
| _0011_1 | R3c e6-off selfjoin 1-part | 143 | SKIPPED_GUARD |
| _0012_1 | R3d e6-on selfjoin 1-part | 78 | SKIPPED_GUARD |
| _0013_1 | R4 e1-more works×sources | 2,917 | join_source_dim measured; selfjoin_source SKIPPED_GUARD |

- **R1 join_meta (4-part) — MEASURED.** Output 5,368,834 rows; measured max
  partition load **43,348**; measured PASS/PASS. Synopsis predicted max
  43,187 → **Q 1.0037**; end-biased predicted 47,937 → **Q 1.1059** (both
  PASS/PASS, agree). **Uniform predicted 110,000 → FAIL/FAIL: a false
  positive on a measured PASS case (Q 2.5376)** — the third uniform false
  positive in E1 (after join_dim at 4 and 8 parts).
- **R1 selfjoin (4-part) — SKIPPED_GUARD with exact predictions.**
  Synopsis arm: predicted output = Σcount² = **795,242,814** (exact; replaces
  the earlier top-30 lower bound ≥ 624,940,052, which it satisfies);
  predicted max partition load **122,469,088** (4.90× the 25M guard cap;
  7.70× the case's absolute verdict limit, 9.86× the relative limit);
  all three arms predict FAIL/FAIL. Guard reason string records both caps.
- **R2 1-part selfjoin — SKIPPED_GUARD; NOT MEASURED.** At 1 part
  (1,260,852 fact rows) the synopsis prediction is still max load
  **43,344,257** (1.73× the 25M cap), Σcount² = 262,464,138; all arms
  FAIL/FAIL. The guard therefore prevented the measurement at this scale
  too. **No executed author_id self-join measurement exists at any scale.**
- **R3 E6 gaps — closed for join_meta; guard-skipped for selfjoin.**
  join_meta AQE off/on: partition-load arrays **bit-identical** across
  conditions and identical to R1's independent E1 measurement (128 loads,
  sum 5,368,834, max 43,348); PASS/PASS both; skew-join optimisation flag
  False both; AQE shuffle-read flag False both. 1-part selfjoin AQE off/on:
  both SKIPPED_GUARD with identical predictions (43,344,257 /
  262,464,138) — AQE never engaged because the case never executed.
- **R4 works×sources (second natural family) — MEASURED.**
  join_source_dim: output 1,405,314 rows; measured max partition load
  **171,026**; measured **FAIL/FAIL**. Synopsis predicted 173,024 →
  **Q 1.0117**, FAIL/FAIL (agrees). End-biased predicted 172,099 →
  **Q 1.0063**, FAIL/FAIL (agrees). Uniform predicted 420,000 → Q 2.4558,
  FAIL/FAIL (verdict agrees, load over-predicted). This is a **measured**
  join-emergent FAIL on naturally occurring data in a second join family,
  called correctly by both heavy-hitter arms.
  selfjoin_source: SKIPPED_GUARD — predicted Σcount² = 44,878,025,678 and
  predicted max load 27,160,194,356 exceed **both** caps (2B output,
  25M max-load); all arms FAIL/FAIL.

### 3. Kiro's patched scripts — ABSENT
The capture contains no code (zip listing checked: no `.py`, no scripts
directory). The (a) streaming-counting / (b) guard-extension / (c) `--cases`
diff against `emr_package_final/code/` **cannot be performed**. Indirect
evidence only: the returned JSONs carry the patched code's signatures
(`status: SKIPPED_GUARD`, dual-cap reason strings, predictions always
recorded), and its predictions are consistent with the original prediction
math (exact Σcount² accounting; predicted max load ≥ the top key's c² =
122,235,136 at 4 parts). Consistency is not a code diff; the item stays
**unverified as code**.

### 4. Failed applications' event logs (_0005/_0006/_0007, cluster A) — ABSENT
Not in this capture (cluster-A event logs present: _0003 smoke, _0004 prep,
_0008 Step 6). The Nov 25 failure classification (observed execution
failure, resource/materialisation class, from complete step logs) is
**unchanged and not upgraded**.

### 5. November as-run retrieval — ABSENT
No November as-run scripts, no counts.npz, anywhere in the capture or in
Drive. **C25 / M9 archival half stays OPEN.**

## Frozen fill-rule outcomes
- **C28 — DOES NOT CLOSE; remains PARTIAL.** The frozen rule requires the
  1-part *measured* self-join AND the guard record. Guard records are now
  present and verified at three cases (author self-join at 4 parts and
  1 part; source self-join), with exact predictions replacing all bounds.
  The measured self-join does not exist: the guard skipped it at every
  scale, because the predicted max load exceeds the 25M cap even at
  1 part (43,344,257). The rule's concealment component remains
  unevaluated for the self-join. What the evidence now supports, exactly:
  (i) join-emergent pressure on natural data is *measured* in the second
  family (join_source_dim FAIL, both HH arms correct, Q ≤ 1.012);
  (ii) the author self-join is predicted FAIL by all arms on exact
  (not bounded) fan-out accounting at two scales, was observed to fail
  execution three times in the original runs and once more in Step 6,
  and is guard-classified as beyond the pre-registered execution caps.
  No stronger statement is licensed.
- **C29 — FILLED for all executable scope.** join_meta pair added
  (identical under AQE off/on); the 1-part selfjoin pair is guard-skipped
  under both conditions — AQE had no executable case there, and none is
  claimed.
- **C25 — OPEN** (item 5).

## Verdict consequence
Classification remains **CONDITIONAL GO**; confidence unchanged
(conditional 60–70% / unconditional 45–55%). The return strengthened E1
materially (exact self-join accounting, a measured second-family FAIL,
a third uniform false positive, join_dim_s8 fully verified) but closed
neither C28's measured component nor C25.
