# VERIFICATION REPORT — Measured Self-Join Return (2026-11-26)

Capture: `EM_Results_selfjoin_measured_20261126-013345Z.zip` (Drive
EM_Results id `1Z2YTAbp5TGakAoUI2-0THdk-EjKl03kX`, 588,841 B, md5
`de0d05df3255a053d98bfb2a4e7bb472` — download read-back matches
exactly). Local tree: `emr_results_selfjoin_measured/capture/`.
All numbers below were recomputed from the raw result JSONs and the
Spark event logs, never from the capture's README prose (the README's
numbers were checked against the raw artifacts and agree).

Both runs executed on cluster **<CLUSTER_ID>**, Spark
**3.5.8-amzn-0**, AQE off, 128 shuffle partitions, with
`--cases selfjoin --force-measure` (step command lines in the
controller logs). The guard was overridden for these two runs only;
predictions were computed and recorded before execution (the JSONs
carry `guard_overridden: true` and the would-trip reason: predicted
max load vs the 25,000,000 cap at both scales).

## Run 1 — 1-part self-join (app <APPLICATION_ID>)

Step <STEP_ID>, exit code 0. Data: `Access/Final_1part`
(left rows 1,260,852). JSON md5 `6a59385416aeb9adb2c8e7fd38797749`;
event log md5 `7fce3f0d6f343f78d62f19ab9e3d8546`.

- **Status: OK — the job COMPLETED.** Event log: 7/7 jobs
  JobSucceeded, 12 stages completed, **453 tasks, 0 failed**,
  **18 executors on 18 distinct hosts**, spill **0 B** memory /
  0 B disk; shuffle read 110,996,288 B, write 61,651,953 B;
  application duration 216 s (JSON elapsed 198 s).
- Measured total output rows **262,464,138** — exactly the predicted
  Σcount² (difference 0). The 128-element per-partition array sums
  to the total exactly; its maximum equals the reported max.
- Measured max partition load **43,340,706** vs predicted
  43,344,257 → **Q (predicted/measured) = 1.000082**. Median
  partition load 217,340 (recomputed from the array, matches).
- **Verdict FAIL/FAIL as measured**: max exceeds the absolute limit
  (0.02N = 5,249,282.76) by **8.26×** and the relative limit
  (2× mean = 4,101,002.16) by **10.57×**.
- Arm predictions vs the measured max: synopsis 43,344,257
  (Q 1.000082, FAIL/FAIL); end-biased 44,412,724 (Q 1.024735,
  FAIL/FAIL); uniform 50,090,000 (Q 1.155726, FAIL/FAIL).
  **No arm conceals the load.**

## Run 2 — 4-part self-join (app <APPLICATION_ID>)

Step <STEP_ID>, exit code 0. Data: `Access/Final`
(Step 1 prep tables; left rows 5,368,834). JSON md5
`7ac3c12f8270ae59cd5d3d36fe9cb709`; event log md5
`d65114bda2de4c8121f179ea6aa1f9b6`.

- **Status: OK — the job COMPLETED.** Event log: 7/7 jobs
  JobSucceeded, 12 stages completed, **608 tasks, 0 failed**,
  **18 executors on 18 distinct hosts**, spill **0 B** memory /
  0 B disk; shuffle read 488,947,508 B, write 270,992,288 B;
  application duration 405 s (JSON elapsed 387 s).
- Measured total output rows **795,242,814** — exactly the predicted
  Σcount² (difference 0). Array sums and max check as above.
- Measured max partition load **123,158,197** vs predicted
  122,469,088 → **Q (predicted/measured) = 0.994405** (measured
  0.56% above prediction; the hottest partition also carries
  co-hashed keys beyond the top key's own 122,235,136 rows).
  Median partition load 1,519,986.5 (recomputed, matches).
- **Verdict FAIL/FAIL as measured**: max exceeds the absolute limit
  (0.02N = 15,904,856.28) by **7.74×** and the relative limit
  (2× mean = 12,425,668.97) by **9.91×**.
- Arm predictions vs the measured max: synopsis 122,469,088
  (Q 0.994405, FAIL/FAIL); end-biased 126,895,136 (Q 1.030343,
  FAIL/FAIL); uniform 89,550,000 (Q 0.727114, FAIL/FAIL — uniform
  understates the max load by 27% but its verdict is still FAIL).
  **No arm conceals the verdict.**

## Decision rule (C28) — CLOSED at both scales

The frozen rule's two outstanding components are now evaluated:
(i) the measured-max component — measured at 1 part and 4 parts,
FAIL/FAIL at both, Q 1.000082 / 0.994405 on the synopsis prediction;
(ii) the concealment component — all three arms predict FAIL at
both scales against the measured ground truth, so no arm conceals
the emergent load. **C28 → VERIFIED.** The four earlier observed
execution failures stand in the record as the pre-measurement
history (the counting-path defect they exposed is what the patched
streaming counter fixed).

## Patched scripts — RECEIVED; diff performed (G03 CLOSED)

The capture includes `patched_scripts/` (common.py md5
`c5cd8d7f09fbfa7bde4daeae14b13af3`, e1_join.py `eef0a4a6012ee9f3873
ba8f34a51a849`, e1_more.py `950b873f352842ce04c4e10bbd6d6053`,
e6_aqe.py `8e3c2546ffa97fd12d958b1b86e699c9`), diffed line-by-line
against `emr_package_final/code/`:

- **common.py — additions only.** Guard constants
  (MAX_OUTPUT_ROWS = 2,000,000,000; MAX_LOAD_ROWS = 25,000,000) and
  `count_per_partition` (streaming `mapPartitionsWithIndex` count
  replacing `glom().map(len).collect()`). `predict_from_counts` and
  `verdict` are **byte-identical** to the original package.
- **e1_join.py** — (a) counting via `count_per_partition`;
  (b) guard extended to the max-load cap, predictions always
  recorded, `--force-measure` override that records
  `guard_overridden` + the would-trip reason without changing any
  threshold; (c) `--cases` / `--out-prefix` / `--out-name`
  arguments. Prediction computation untouched.
- **e1_more.py** — `--cases` block selector + a staging-reuse
  helper (same 4 largest works parts); inherits run_case from
  e1_join. No prediction code of its own; none changed.
- **e6_aqe.py** — prediction + guard block added using the same
  `predict_from_counts`; join_meta case added; `--cases` selector;
  streaming counting. No force override (the E6 self-join stays
  guard-skipped by design).

**Conclusion: the patch is exactly (a) streaming counting,
(b) guard extension + recording, (c) case selection, plus the
author-ordered `--force-measure` override. Prediction math is
unchanged.** The measured runs above were executed by this code
(step command lines confirm `--py-files .../common.py .../e1_join.py
--cases selfjoin --force-measure`).

## Still absent

- The November EMR as-run code (both hand-fix bootstraps) — **C25
  stays OPEN**; unrelated to this capture and unchanged by it.
- Event logs for the three failed November-campaign applications
  (_0005/_0006/_0007) — still absent; their failure classification
  continues to rest on the complete step logs, as disclosed. (This
  capture's event logs cover the two NEW applications, which are
  the ones the revision cites as measured.)
