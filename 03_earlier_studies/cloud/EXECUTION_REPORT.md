# Blind at Scale cloud program — execution report (overnight, Nov 22–23, 2026)

Hard cap $75 ($50 Databricks / $25 Snowflake). Databricks spend:
**<$8.82 worst-case bound** of $50 (trial 50 DBU/hr cap x wall time;
`system.billing.usage` returned 0 rows all night -- trial accounts do
not appear to populate it, so no measured DBU figures exist; actual
spend is expected to be far below the bound). Snowflake spend: **$0.00**
of $25 (blocked, see below). No cap was approached; nothing was trimmed
for money. [SUPERSEDED for Snowflake — see the addendum at the end of
this file: the key was registered 2026-11-23 00:12 EST and both
Snowflake arms executed 00:13–00:35 EST, metered $0.92.]

## Arms completed (all Databricks; Spark 4.2.0, serverless Jobs compute)

### D-DBX — real-engine GT: DONE. Multi-node claim: VOID.
9/9 verdicts agree with the single-host pod GT; loads exact (G/W) or
within 0.10% (J; max 0.098% on J_B — corrected from 0.014% per R5 N1);
F analytic loads exact. Local sim 9/9, median Q
1.125; A5 9/9 Q 1.081; A3 4/9 (same 5 disagreements as pod). Threshold
audit 9/9 at T=200,000. Bit-identical across 4 consecutive successful
runs. **Topology: the pre-registered hostname gate is NOT satisfied.**
statusTracker is dead under Spark Connect, the event log never lands
in the volume, and hostnames are MASKED (gethostname() /
/proc/sys/kernel/hostname return ""). The executor-identity UDF probe
(P9/P10) shows exactly ONE executor (0c173ff1-83f5-4d32-8f47-ebc68a9763ce),
sampled before AND after the pipelines, the same executor across
separate jobs. The paper may say the GT was validated on Databricks
serverless Spark -- WITHOUT any multi-node claim. The R2 item stays
open; fallbacks (classic-cluster paid upgrade -- blocked on this trial,
no worker environments; RunPod 3-pod route; partner re-attempt) are the
author's decision. D9 (Photon): UNKNOWN, config unreadable on serverless.

### D-SCALE 40M — DONE. A5 9/9 Q 1.081; A1 7/9 (misses J_A, J_N).
Uploaded local 40M data; T=800,000; threshold audit 9/9; local sim 9/9
Q 1.120. 1 executor.

### D-SCALE 100M — DONE. A5 9/9 Q 1.085; A8 9/9 Q 1.089.
Generated on-cluster in 57.9 s (no OOM -- driver memory fits 100M).
P11: numpy gate relaxed for the 100M leg only (no local anchor);
**re-derivation under numpy 1.23.5 reproduced the cluster's
counts_sha256 exactly** (all 8 checks PASS). Local arms run on the VM
(study code, numpy 2.5.4, counts + merged distinct_A). Synopsis
101,586 B at 100M. Ladder (A5 median Q): 1.081 / 1.081 / 1.085 --
fidelity holds at 100M. 1 executor.

### D-FB — DONE. 7/7 repairs executed-verified on the real engine.
G_A 1,086,736 -> 165,035; G_N 1,043,904 -> 158,297; J_A/J_N -> 181;
F_A 546,740,246,171 -> 181; F_B 3,719,508,913 -> 181; W_A -> 165,035.
Salt determinism PASS (vectors exactly equal). Sample outeq spot check
(1,001,299 rows): G_A hash equal, J_A totals + per-key hash equal.

### D-NEG — DONE. Negative family confirmed on the real engine.
All 4 V pipelines FAIL on executed byte loads (V1 12,184,965; V2
28,143,694; V3 1,254,205,218; V4 279,451,622,782), matching local GT
verdicts 4/4. A5-NF and A8-NF: 0/4 agreement, recall 0.000 (measured,
not asserted).

## Arms NOT run — Snowflake blocked on key registration
S-XENG and S-SIZE: the BAS public key is still not registered on
CHALAKONENI (server returns `390144 JWT token is invalid`; the key
pair itself verified self-consistent tonight). Registration statement:
`results/snowflake_registration.sql`. Also found: the connector needs
account `giobwtb-urc15524` (bare `URC15524` 404s). Full note:
`results/s-xeng/SNOWFLAKE_STATUS.md`. When registration lands, the
run order is in the README; everything is prepared (venv, SQL, local
parquet verified, driver selftest passed).

## Execution patches (package untouched; PATCH_NOTES.md P1–P13)
P1 Photon-guard; P2 upload fallback; P3 classic workspace import; P4 UC
via own SQL warehouse; P5 Spark Connect guards; P6 empty env (numpy
2.5.4 uninstallable on Python 3.11; numpy 2.4.6 ABI-breaks the base
env); P7 no persist/clearCache on serverless; P8 hostname UDF
(superseded); P9 executor-identity UDF; P10 post-pipelines re-sample;
P11 numpy gate relaxed for 100M only; P12 npz via local temp (FUSE EIO
on zipfile seeks); P13 `args.sample_outeq` typo fix (package bug).
Patched scripts: spark_pipelines_dbx.py ce02b383..., feedback_dbx.py
3e513fa7..., negfam_dbx.py e1d29851..., gen_on_dbx.py 9499e8c2....

## Artefacts
All under `cloud_program/results/<arm>/` with SHA-256 manifests
(MANIFEST.sha256, all verified 0 non-OK, 86 files): run manifests, GT
JSONs, cluster_comparison JSONs, per-arm reports (D_DBX_REPORT.md,
D_SCALE_40M_REPORT.md, D_SCALE_100M_REPORT.md, D_FB_REPORT.md,
D_NEG_REPORT.md), task outputs, request logs, the 100M dataset +
exports + both venvs. Spend ledger: results/SPEND_LEDGER.md.
Token-trace check: no trace of the Databricks token in any results
file (grep clean). No manuscript, NUMBER_PROVENANCE, or study file
was edited. No vendor was contacted.

## Parent actions outstanding
1. Register the Snowflake BAS public key (then S-XENG/S-SIZE can run;
   ~$25 sub-cap untouched, Snowflake trial ends ~Dec 22).
2. Browser check of the Databricks trial balance + end date (API
   cannot see it; billing table unpopulated).
3. Decide the R2 multi-node fallback (classic-cluster paid upgrade,
   RunPod 3-pod route, or partner re-attempt) -- D-DBX cannot retire
   it from this serverless trial.

## Addendum 2026-11-23 (Snowflake arms executed; written at integration)

Parent action 1 above is DONE: the BAS public key was registered on
CHALAKONENI (RSA_PUBLIC_KEY_2, rotation slot; VISTA's RSA_PUBLIC_KEY
untouched; fingerprint verified exact) at 2026-11-23 00:12 EST, and
both Snowflake arms executed 00:13–00:35 EST. The "Arms NOT run"
section above is therefore superseded; it is kept as the record of
the Databricks phase.

### S-XENG — cross-engine transfer: DONE (weak at 10M)
Data bit-identical (counts SHA f9f3fa8b…28f exact; F analytic loads
546,740,246,171 / 3,719,508,913 exact). Pre-registered spill proxy:
0 bytes spilled (local and remote) on all 9 pipelines, no errors —
the pre-registered outcome "proxy uninformative at this scale".
Spearman A5 predicted load vs Snowflake elapsed: 0.485 (XS).
The 4/9 "verdict agreement" figures and A3's 9/9 in the comparison
JSON are artifacts of all-PASS proxy verdicts and must never be
cited. Report: results/s-xeng/S_XENG_REPORT.md.

### S-SIZE — warehouse sensitivity: DONE (no boundary movement)
Same 9-pipeline workload on XS/S/M (Gen2): FAIL-proxy counts 0/0/0,
0 bytes spilled on every size — the boundary-movement question has a
vacuous premise at 10M. Elapsed 0.4–1.7 s, noise-dominated (same
queries on XS differ up to ~2.3x between runs (G_A: 471 vs 1,077 ms,
a factor of 2.29); no size-vs-elapsed
trend may be cited. Spearman A5-vs-elapsed across the four
measurements: 0.485 / 0.301 / 0.243 / 0.452 (unstable).
Report: results/s-size/S_SIZE_REPORT.md.

Snowflake spend (metered, final): $0.9242 of $25 (XS 0.161641 cr
$0.4849; S 0.054779 cr $0.1643; M 0.091652 cr $0.2750). Total
campaign: <$9.75 of the $75 cap (corrected from <$9.74 per R5 N9 —
<$8.82 + $0.9242 = <$9.7442). Patches P14–P18 (Snowflake) in
results/exec_patches/PATCH_NOTES.md; package files untouched.
Parent action 3 is also decided: the author chose the partner
re-attempt route (PARTNER_REATTEMPT_NOTE.md, 2026-11-23); the
RunPod 3-pod route remains prepared as fallback.
