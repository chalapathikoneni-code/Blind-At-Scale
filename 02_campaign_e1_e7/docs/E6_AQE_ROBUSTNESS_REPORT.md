# E6 — Spark AQE Robustness: FINAL FOR AVAILABLE SCOPE

**Status: EXECUTED on EMR 2026-11-25 and VERIFIED from the returned
capture (`EMR_FINAL_CAPTURE_VERIFICATION.md`).** AQE off vs on pairs
completed and verified for: the natural `join_dim` case (Steps 3/4)
and the synthetic skew cases J_A / J_N (Steps 7a/7b). The natural
selfjoin pair crashed under **both** conditions (same
resource/materialisation failure as E1); its feasible-scale pair
(1-part) is **PENDING USER RETURN** with the recovery batch (R3).
Design and interpretation were frozen before any outcome
(`FINAL_EXECUTION_PROTOCOL.md` §1; original E6 report).

## 1. Verified results

### join_dim (OpenAlex, fan-out 1) — AQE changed nothing
| | AQE off (Step 3) | AQE on (Step 4) |
|---|---|---|
| max partition load | 51,961 | 51,961 |
| output rows | 5,368,834 | 5,368,834 |
| verdict (rel/abs) | PASS / PASS | PASS / PASS |
| elapsed | 11.1 s | 11.2 s |
| plan: AQEShuffleRead / skew-join opt | False / False | False / False |

The 128 per-partition loads are bit-identical across conditions and
identical to Step 2's independent (non-E6) measurement of the same
join. Recorded caveat: the on-run's plan text is the *initial*
adaptive plan (`isFinalPlan=false`), so the plan-string check alone
would be weak evidence; the decisive evidence is the physical
identity of the executed loads.

### Synthetic skew J_A / J_N (100M) — AQE did not engage; FAILs persist bit-identical
Event-log verified (apps _1795491295161_0004 off / _0005 on; 18
executors / 18 hosts each; 548 tasks each; 0 failed tasks; spill 0 B
in both conditions):
| Case | max load off → on | median off → on | verdict off → on | elapsed off → on |
|---|---|---|---|---|
| J_A | 10,873,845 → 10,873,845 | 528,284.5 → 528,284.5 | FAIL/FAIL → FAIL/FAIL | 47.1 s → 47.7 s |
| J_N | 10,442,419 → 10,442,419 | 506,570.5 → 506,570.5 | FAIL/FAIL → FAIL/FAIL | 43.3 s → 41.6 s |

Partition-load arrays identical per case; application-level shuffle
totals identical **to the byte** (read = write = 5,039,703,273 B /
202,876,741 records in both applications). No coalescing, no skew
split, no metric movement of any kind: on these predicted-hot cases,
AQE-ON is execution-identical to AQE-OFF. (Today's J_N spill is 0 B
vs 748,309,212 B in the November full-pipeline run — different job
shape; no cross-job spill claim is made.) Today's executed maxima
also reconcile exactly with the November GT (each 154 records below
the GT shuffle-read load = the dim-side records in the fullest task).

### Selfjoin (OpenAlex) — both conditions failed identically; AQE did not rescue
AQE-off (Step 3) and AQE-on (Step 4) both died in the selfjoin's
shuffle/materialisation distress (signatures in the verification
report §6; the on-run: DataStreamer saturation from 18:55:43 UTC,
executor 6 killed exit 143 in stage 10, heartbeat cascade, context
shutdown at 19:14:11). Skew-join optimisation is absent from the
on-run's recorded plan evidence. Statement supported: **on the one
natural case where mitigation mattered most, AQE as configured did
not prevent the failure.** The paired comparison at a feasible scale
(1-part, off vs on, with plan evidence per run) is the recovery R3
slot and remains pending.

## 2. Frozen interpretation — applied

The frozen question was whether the predicted pressure remains
operationally meaningful when the engine can mitigate skew at
runtime. On the verified scope the answer is the strong branch of
the frozen interpretation: **AQE did not engage on any completed
case** (plan evidence + physical identity, two independent
instruments), **the predicted-hot synthetic cases' FAIL loads persist
bit-identical under AQE-ON**, and **the natural emergent case failed
under both conditions**. The prediction's operational meaning
therefore stands on this engine/configuration: pre-execution
prediction is not substituting for a mitigation the engine would
have applied anyway. Scope limits, stated plainly: one engine
(Spark 3.5.8-amzn-0), one configuration family (the package's frozen
confs; within each pair the ONLY difference is
`spark.sql.adaptive.enabled`, verified from the captured step
commands), ShuffledHashJoin plans throughout; the selfjoin pair's
feasible-scale completion is still pending and could extend — not,
on current evidence, be expected to reverse — this statement.

## 3. Pending slots (explicit)

| Item | Arrives with |
|---|---|
| 1-part selfjoin AQE off vs on (measured loads, spill, plan evidence, skew-join engagement) | Recovery R3 |
| Failed applications' event logs (_0006/_0007) for stage-level AQE-era metrics, if still in S3 | Next return |

---

## 4. FINAL RETURN UPDATE — 2026-11-26 (COMPLETE capture, verified)

- **join_meta AQE pair (R3a/R3b): MEASURED, identical.** Partition-load
  arrays bit-identical off vs on and identical to R1's independent E1
  measurement (max 43,348, PASS/PASS both); shuffle write byte-identical
  (257,923,472 B in both applications' event logs); skew-join flag
  False both; AQE shuffle-read flag False both.
- **1-part selfjoin AQE pair (R3c/R3d): SKIPPED_GUARD under BOTH
  conditions**, identical predictions (43,344,257 / 262,464,138). AQE
  never engaged because the case never executed; no AQE claim is made
  for the self-join beyond "the guard classified it identically under
  both conditions."

**C29 after the final return: FILLED for all executable scope.** Every
case that can execute under the pre-registered caps shows AQE changing
nothing physically; the one remaining case is guard-skipped under
both conditions.
