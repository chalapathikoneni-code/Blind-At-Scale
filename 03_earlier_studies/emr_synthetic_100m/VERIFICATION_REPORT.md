# Verification Report — Synthetic 100M EMR Run (Blind at Scale, R2 multi-node evidence)

**Date:** 2026-11-23 (verified 13:32–13:50 EST)
**Artifact:** `synthetic_s3.zip`, Drive id `1PySbYWnijnSGHcaZj0tdmU5LZJ17sXsY`,
680,001,536 B, md5 `76e066991c9d5f38840ac3e3e9cf8c64`, uploaded 2026-11-23 13:29 EST.
Parent folder `1P3X1PAQXMSWpEvBr73QNTC2aue4HksLv` contains this file ONLY (no siblings).
**Local copy:** `~/workspace/blind-at-scale/emr_results_synthetic/synthetic_s3.zip`
(download md5 verified exact).

## Provenance verdict: VOID as delivered

**The run is VOID as R2 multi-node evidence — not because the run failed, but
because the delivered artifact does not contain the evidence.** The zip is
truncated and holds only the package docs plus the generated corpus (one file
partial). It contains no Phase 1 output at all: no ground-truth JSON, no
per-pipeline manifests, no Spark event log, no verifier output, no step logs.
Placement (the R2 proof) cannot be established from this artifact: host count
is UNKNOWN. No comparison against the Databricks ground truth is possible:
there are no EMR loads to compare.

What the artifact DOES establish (see §3–§4): Phase 0 executed on the cluster
— the 100M-row corpus was generated and passed the counts SHA gate on EMR.
That is a real, verified result, but it is a dataset-identity result, not a
multi-node result.

## 1. The zip is truncated (established from the bytes, not assumed)

- No central directory exists anywhere in the file (zero `PK\x01\x02`
  signatures in a full scan) and no end-of-central-directory record
  (`PK\x05\x06` absent). `unzip` and Python `zipfile` both refuse the file.
- 8 local file headers only. The last entry, `synthetic/data/fact.parquet`,
  declares 1,381,159,996 compressed bytes but only 462,802,219 are present
  (33.5%); the file ends mid-entry. The header declares the FULL size, so the
  source file was complete when zipping started — the zip process was
  interrupted partway through `fact.parquet` (creation-side truncation, not a
  download fault: the md5 matches Drive exactly).
- Recovery was done by parsing the local headers manually and inflating each
  complete deflate stream, verifying CRC-32 per entry. 4 of 5 files recovered
  complete and CRC-clean; `fact.parquet` recovered to 501,771,481 of
  1,497,803,378 decompressed bytes (33.5%), saved as
  `recovered/synthetic/data/fact.parquet.PARTIAL`.

## 2. Inventory (all 8 zip entries)

| Entry | Declared size (uncompressed) | Status |
|---|---|---|
| `synthetic/` | dir | OK |
| `synthetic/docs/` | dir | OK |
| `synthetic/docs/VALIDATION_REPORT.md` | 15,868 B | OK, CRC match |
| `synthetic/docs/README_RUNBOOK.md` | 18,364 B | OK, CRC match |
| `synthetic/data/` | dir | OK |
| `synthetic/data/dim.parquet` | 147,512 B | OK, CRC match |
| `synthetic/data/fact2.parquet` | 234,880,935 B | OK, CRC match |
| `synthetic/data/fact.parquet` | 1,497,803,378 B | TRUNCATED at 33.5% |

**Absent from the artifact (expected S3 contents per the job code, all missing):**
`data/counts.npz`, `data/counts_summary.json`, `data/meta.json`,
`output/run_manifest.json`, `output/job_summary.json`, `eventlog/<app-id>`,
`code/`, `config/`, `logs/`, and the verifier's local outputs
(`emr_verify_out/emr_ground_truth_100m.json`, `emr_verify_out/verification_report.json`).
Whether these exist in S3 is unknown from this artifact — they lie beyond the
truncation point in the zip's (readdir-order) entry sequence, so their absence
here proves nothing about S3.

## 3. Package version: v1 docs (byte-identical)

Both recovered docs are SHA-256-identical to the v1 package
(`emr_package.zip`, SHA `62d08a15…`, Drive id `15mQlvzx9gnXRExPrcu1jbZjTzm-KGcdz`):

- `VALIDATION_REPORT.md`: `51db068cb4b4ae814e786a582194054b6d1eb75f044cd97e55c0cd471948bf44`
- `README_RUNBOOK.md`: `871d7cfd65ac0265ae8de4b85750212a959aa37ebe67faf48f779c607f460aa4`

No v2/v2.1 markers anywhere (no vendored-wheels, OFFLINE, `--break-system-packages`,
or `verify_as_hadoop` text). The S3 prefix's `docs/` were last uploaded from v1.
**Which bootstrap actually ran is UNDETERMINED from this artifact** (no bootstrap
or step logs): the successful Phase 0 proves *some* working bootstrap got
numpy 1.23.5 visible to the job — v2.1's, or a hand-fix on the laptop — but the
artifact cannot distinguish them.

## 4. Counts SHA gate: INDIRECT PASS; direct recomputation impossible here

**The gate's position in the code is the proof.** In `code/spark_job.py`,
`generate_and_upload()` runs the SHA gate (lines 314–329, `SystemExit` on any
mismatch against `EXPECTED_COUNTS_SHA256 =
3549c275165e4e75c7de2e74c1b8d912cba4ee54141d7ff8db3abc36cd11dd8f` and the 7
headline maxima) BEFORE writing parquet (lines 362–364) and BEFORE any S3
upload (lines 388–391). The corpus's presence in S3 therefore entails the gate
passed on the cluster with the anchor SHA — and, transitively, that numpy
1.23.5 was in effect (any other numpy version produces different draws and
fails the gate; verified locally for 2.5.4).

**Independent corroboration from the recovered parquet** (read with pyarrow
26.0.1; reading does not depend on the numpy version):

| Check | Expected (anchor) | Observed | Result |
|---|---|---|---|
| dim rows | 20,000 | 20,000 | EXACT |
| dim keys | 0–19,999 | 0–19,999 | EXACT |
| fact2 rows | 60,000,000 | 60,000,000 | EXACT |
| fact2 kA max | 5,726,027 | 5,726,027 at key 0 | EXACT |
| fact2 kB max | 463,727 | 463,727 at key 0 | EXACT |
| fact2 nonzero keys (kA/kB) | 20,000 / 20,000 | 20,000 / 20,000 | EXACT |
| fact2 top-5 kA | — | 5726027, 2859698, 1907242, 1430240, 1146175 | recorded |
| parquet writer | pyarrow 14.0.2 | `parquet-cpp-arrow version 14.0.2` (dim + fact2) | EXACT |
| fact.parquet | 100,000,000 rows | TRUNCATED in zip; valid `PAR1` magic at offset 0; footer absent | UNVERIFIABLE here |

The fact2 maxima are extreme-value statistics of the seeded draws; matching to
the unit is conclusive corroboration that the draws are bit-identical to the
anchor. Recomputed fact2 count vectors saved as `fact2_counts.npz` in this
directory. Full SHA recomputation is impossible from this artifact: it needs
fact's four count vectors (kA/kB/kC/kN) and `counts.npz` is absent.

## 5. Placement (the R2 proof): NOT ESTABLISHED

No event log exists in the artifact. Distinct executor hostnames: UNKNOWN.
Task hosts: UNKNOWN. Executor count: UNKNOWN. The `placement` gate
(`verify_emr_run.py`) cannot run. **No multi-node claim of any kind can be
supported by this delivery.** (No hostnames appear anywhere in the artifact, so
there is nothing to redact.)

## 6. Environment fingerprint

| Field | Value | Source |
|---|---|---|
| pyarrow (parquet writer) | 14.0.2 | `created_by` in dim + fact2 footers |
| numpy (job) | 1.23.5 (entailed, not directly observed) | SHA gate passing entails it; no log records it |
| Dataset | zipf seed 101, 100M/60M/20k | dim + fact2 verified; fact unverifiable here |
| EMR release | UNKNOWN | no manifest / step log |
| Spark version | UNKNOWN | no manifest / step log |
| Cluster id / step id | UNKNOWN | no manifest / step log |
| Timestamps | UNKNOWN | no manifest / step log |
| Executor count / topology | UNKNOWN | no manifest / event log |
| Package version (docs) | v1 (byte-identical) | §3 |
| Bootstrap that ran | UNDETERMINED | no bootstrap / step log |

## 7. Verifier verdict: ABSENT

No `verification_report.json`, no `emr_ground_truth_100m.json`. All six
verifier gates (`counts_sha_manifest`, `counts_sha_npz`, `placement`,
`all_9_pipelines`, `threshold_audit`, `d8_cross_check`) are unevaluable from
this artifact.

## 8. Comparison vs Databricks 100M GT: CANNOT BE DONE

There are no EMR ground-truth loads in the artifact, so no verdict agreement
and no Q-errors can be recomputed. **No comparison table is produced here** —
producing one would require inventing EMR numbers. For reference only (these
are DATABRICKS numbers, NOT EMR results), the Databricks 100M GT loads at
T=2,000,000 are: G_A 10,873,845 FAIL; G_B 1,571,791 PASS; G_N 10,442,419 FAIL;
J_A 10,873,845 FAIL; J_B 1,571,791 PASS; J_N 10,442,419 FAIL;
F_A 54,690,910,911,173 FAIL; F_B 371,507,261,637 FAIL; W_A 10,873,845 FAIL
(source: `emr_package/config/dbx_gt_100m.json`). The local arm predictions
(`config/predictions_main.csv`, 66 rows) are on file and ready for the
comparison the moment EMR loads arrive.

## 9. What can enter the manuscript (and what cannot)

**CAN (with the stated labelling):**
- Phase 0 executed on EMR: the 100M-row zipf/s101 corpus was generated on the
  cluster and passed the counts SHA gate against the Databricks anchor
  (`3549c275…`). Evidence: gate-before-upload ordering in the job code +
  fact2 maxima/dim counts independently verified from the recovered corpus.
  This is a dataset-identity statement only.

**CANNOT (no evidence in this artifact):**
- Any multi-node / multi-host claim (no event log, no placement proof).
- Any ground-truth load, verdict, or verdict-agreement figure (no GT JSON).
- Any Q-error / load-fidelity figure for any arm (no EMR loads).
- Any Spark/EMR environment claim beyond pyarrow 14.0.2 (no manifest/logs).
- Any statement that v2.1 ran (docs are v1; bootstrap undetermined).

## 10. What is needed to complete the verification

From the S3 prefix (all small files — the 1.8 GB parquet need NOT be re-sent;
dim + fact2 are verified here and fact's counts are vouched for by the gate):

1. `output/run_manifest.json` — app id, Spark version, counts SHA, all 9 loads, topology
2. `output/job_summary.json`
3. `eventlog/<app-id>` — THE placement proof (executor hostnames)
4. `data/counts.npz`, `data/counts_summary.json`, `data/meta.json` — direct SHA recomputation
5. If the verifier was run locally: `emr_verify_out/verification_report.json` and `emr_verify_out/emr_ground_truth_100m.json`

Practical note: zip ONLY those small files (KBs–MBs), or if re-zipping the full
prefix, run `unzip -l` on the zip before uploading — a zip without a central
directory is unreadable, and this delivery cost a full verification round.

---

# Round 2 — Phase 1 evidence delivered (verified 2026-11-23 20:54–21:15 EST)

**New delivery:** Drive folder `1hXQuHHCTxQ-FCveZQ8nRNU_FcZbYaOx7` (uploaded
2026-11-23 ~20:50 EST). Local copies in
`~/workspace/blind-at-scale/emr_results_synthetic/round2/`.

| File | Drive id | Size (B) | md5 (Drive) | Verified |
|---|---|---|---|---|
| `eventlog.zip` | `1wjJPp4UdO3rgfu8DDgWHeIHQMgpaszSm` | 1,985,024 | `2c2cf7118b4392d7be6601f8161f1369` | exact; `unzip -t` clean |
| `logs.zip` | `1p3KU-i2fncgSEEV0VZH4R_qQ_MgEaV_q` | 15,798,989 | `78e9c04bbce0ab36004c8db1560192f4` | exact; `unzip -t` clean |
| `output/run_manifest.json` | `1D9C-UiuhPVLK9NebV0gNtc4CISwtuzQe` | 18,484 | `a3eab61af1881ff74e8158c60b5c7688` | exact (see note) |
| `output/job_summary.json` | `1xsQqxevp84UoA2GiJ1acufAbvBWsG2kN` | 2,577 | `3622168a1426e05752ccbc11abc1f909` | exact (see note) |

**Download note (JSON files):** the Drive CLI prints JSON responses to stdout
instead of writing bytes (`--output` captures binary responses only), so both
JSON downloads initially landed as 0-byte files. The files were reconstructed
by parsing the CLI's stdout, dropping its injected `retrieved_at` key, and
re-serializing with `json.dump(indent=1)` in the job code's exact dict
insertion order (from `spark_job.py`: manifest, counts_summary, environment,
topology, measurement, per-pipeline entry orders). Both reconstructions match
Drive's size AND md5 byte-exactly, so the analyzed files are provably the
delivered files. Both zips list and test cleanly — unlike round 1, there is
no truncation anywhere in this delivery.

## Round-2 provenance verdict: GO

**The run is VERIFIED as R2 multi-node evidence.** The package's own verifier
(`emr_package/scripts/verify_emr_run.py`, run with `--skip-download` on the
delivered files) exits 0 with every evaluable gate PASS: counts SHA exact,
placement on **18 distinct executor hosts**, 9/9 pipelines with event-log
ground truth, threshold audit 9/9, D8 cross-check within 0.01%. EMR ground
truth agrees with the Databricks 100M ground truth on 9/9 verdicts with load
ratios 1.0000–1.0001. **This run retires the R2 multi-node objection.**
Two caveats, both recorded honestly below and neither blocking the verdict:
(1) the bootstrap that ran was a laptop hand-fix (venv + PYSPARK_PYTHON), not
any packaged version (§R2.5); (2) `counts.npz` was still not delivered, so
direct SHA recomputation remains impossible — the manifest gate + round-1
corpus checks + an independent in-cluster gate print stand in its place
(§R2.3).

## R2.1 Placement (the R2 proof): 18 distinct hosts — PASS

From `eventlog/<APPLICATION_ID>_1` (37,156,335 B, finalized —
`SparkListenerApplicationEnd` present, not `.inprogress`), parsed by the
verifier's `load_events` and independently re-parsed here:

- **18 `SparkListenerExecutorAdded` events, 18 distinct executor hosts.**
  Task hosts (from `SparkListenerTaskEnd`) are the same 18 hosts — every
  executor that ran tasks is accounted for, and no task ran anywhere else.
- **5,036 task ends, 0 failed tasks, 47 stages.**
- Driver (ApplicationMaster) on a 19th core host
  (`ip-10-192-21-151.ec2.internal`, from the manifest's `environment`); the
  master is `10.192.21.116`. Topology accounted for: 1 master + 19 core
  m5.xlarge = 18 executor hosts + 1 driver host + 1 master.
- Executor hosts (recorded here only, never in chat-facing text):
  `ip-10-192-21-6`, `-9`, `-13`, `-42`, `-61`, `-68`, `-101`, `-113`,
  `-117`, `-118`, `-126`, `-131`, `-160`, `-167`, `-174`, `-189`, `-196`,
  `-226` (all `.ec2.internal`).
- The manifest's in-job topology probe failed on this Spark version
  (`statusTracker: AttributeError("'StatusTracker' object has no attribute
  'getExecutorInfos'")`, fallback `TypeError`) and recorded
  `available: false, distinct_hosts: 0` — the event log is the deciding
  audit trail, exactly as the job design specifies. The probe failure is a
  finding about the PySpark API surface, not about the run: it does not
  affect any load or verdict.

## R2.2 Counts SHA gate: EXACT MATCH (manifest); direct recomputation still impossible

- Manifest `counts_sha256` =
  `3549c275165e4e75c7de2e74c1b8d912cba4ee54141d7ff8db3abc36cd11dd8f` —
  **exact match** to the Databricks 100M anchor. `counts_gate: PASS`,
  `headline_expected_check: PASS`. All 7 headline maxima recorded in the
  manifest match the anchor (fact kA 9,537,885; kB 772,471; kC 5,281;
  kN non-null max 9,161,039; fact2 kA 5,726,027; kB 463,727; nulls 4,000,000).
- **Independent in-cluster confirmation:** the failed first attempt's driver
  stdout (app 0001, same cluster — §R2.6) prints `SHA GATE PASS:
  counts_sha256=3549c275…` with numpy=1.23.5. The gate passed on the
  cluster, on the record, before the successful run.
- `data/counts.npz`, `counts_summary.json`, `meta.json` were NOT delivered,
  so the verifier's `counts_sha_npz` sub-gate is SKIP (not FAIL — by the
  verifier's design the manifest gate stands). Direct SHA recomputation
  from count vectors is still impossible from the delivered artifacts.
  What stands in its place: (a) the manifest SHA (exact); (b) the
  gate-before-upload code ordering (round 1 §4); (c) round 1's independent
  verification of the recovered corpus (dim 20,000 exact; fact2 60M exact;
  fact2 maxima exact to the unit); (d) the app-0001 gate print. The missing
  npz is a documentation gap, not a verdict blocker.

## R2.3 Environment fingerprint

| Field | Value | Source |
|---|---|---|
| Cluster id | `<CLUSTER_ID>` | logs.zip paths; a NEW cluster, not j-0207 (see §R2.6) |
| Step id (successful run) | `<STEP_ID>` (step 3) | step controller: exit 0, 228 s |
| App id | `<APPLICATION_ID>` | manifest; event log |
| Spark version | `3.5.8-amzn-0` | manifest; event log |
| Hadoop version | `3.4.2-amzn-2` (compiled 2026-11-09) | NodeManager STARTUP_MSG |
| EMR release label | NOT directly recorded in the delivered artifacts (no `releaseLabel` string anywhere in the logs). Hadoop 3.4.2-amzn-2 + Spark 3.5.8-amzn-0 + AL2023 + Java 17 Corretto are consistent with the emr-7.14.0 the package specifies, but the literal label is not evidenced here — do not cite a release number more precise than the Spark version. |
| Master | `yarn` | manifest; spark-submit `--master yarn --deploy-mode cluster` |
| numpy (job) | `1.23.5` (exact) | manifest; bootstrap VERIFIED line (all 20 nodes); app-0001 stdout |
| pyarrow | `14.0.2` | manifest; bootstrap; parquet writer stamp (round 1) |
| Python | `3.11.18` | manifest; bootstrap venv built from python3.11 |
| Platform | Linux 6.1.190-237.431.amzn2023, glibc 2.34; Java 17 Corretto | manifest; AM launch script |
| Executors | 18 (`--num-executors 18`, 4 cores, 8 g each; driver 8 g; dynamic allocation OFF) | spark-submit command; 18 ExecutorAdded events |
| Shuffle partitions | 128 | manifest; spark-submit conf |
| Cluster timeline (EST) | nodes provisioning ~10:42; bootstrap 10:44; step 1 (no-op) 10:48; step 2 (app 0001, FAILED) 10:49–10:51; step 3 (app 0002, SUCCESS) 13:04–13:07; Spark app lifetime 13:05:20–13:07:50 (149.5 s); Phase 0 gen 53.49 s | step controllers; event log; manifest |
| S3 layout | bucket `YOUR_BUCKET`, prefix `Access/synthetic` | manifest |

## R2.4 Package version that ran: a laptop hand-fix, NOT v1/v2/v2.1

The bootstrap that ran on cluster j-0764 is **none of the packaged
versions**. Evidence (bootstrap-actions logs, all 20 nodes):

- The script builds a venv at `/usr/local/bas-venv` from python3.11 and
  installs `numpy==1.23.5` + `pyarrow==14.0.2` **from PyPI** (wheels
  downloaded at 37–190 MB/s — the nodes reach PyPI via the internal path),
  then prints `BOOTSTRAP OK: venv /usr/local/bas-venv has pinned
  numpy+pyarrow (PYSPARK_PYTHON points here).`
- The step's spark-submit sets `spark.pyspark.python`,
  `spark.yarn.appMasterEnv.PYSPARK_PYTHON`, and
  `spark.executorEnv.PYSPARK_PYTHON` all to `/usr/local/bas-venv/bin/python`.
- Marker counts across all 20 nodes' bootstrap logs: `BOOTSTRAP OK` 20/20,
  `VERIFIED ... numpy=1.23.5 pyarrow=14.0.2` 20/20; `HADOOP-VERIFIED` 0/20,
  `break-system-packages` 0/20, `vendored wheels` 0/20, `verify_as_hadoop`
  0/20. The v2.1 markers are absent everywhere; the v1 user-site failure
  mode is absent too (the venv sidesteps it by construction).
- The bootstrap was fetched from `s3://YOUR_BUCKET/Access/synthetic/code/
  bootstrap_action.sh` (controller log) — i.e. the S3 copy was overwritten
  with the hand-fixed script. The v1 docs in the S3 prefix (round 1 §3)
  are stale uploads; they are not what ran.

**Consequence (honest caveat):** the run itself is valid — every gate passes
and the SHA gate proves the end state (numpy 1.23.5 visible to PySpark) —
but the reproduction package (v2.1) does **not** contain the exact bootstrap
and step submission that executed. The executed artifacts live only in S3
(`code/bootstrap_action.sh`, plus the spark-submit command recorded in the
step controller log). Anyone re-running from the v2.1 package should reach
the same end state by a different route (vendored wheels + system install,
validated locally to produce identical versions), but strict
bit-for-bit reproduction of the *setup* requires archiving the S3
bootstrap script and the step-3 submit command. **Recommendation:** ask the
user/Claude to send `s3://YOUR_BUCKET/Access/synthetic/code/bootstrap_action.sh`
as it stands, and file it with this report — do not let the executed
bootstrap exist only in S3.

## R2.5 Job health and timeline

Three steps ran on j-0764:

1. **<STEP_ID> (step 1, 10:48 EST, 2 s, exit 0):** a no-op
   debugging step (`echo Debugging step is a no-op`). Irrelevant.
2. **<STEP_ID> (step 2, 10:49–10:51 EST, exit 1, 122 s):**
   app 0001 FAILED — but Phase 0 succeeded first: the driver stdout shows
   `Phase 0: generating zipf seed 101 ... numpy=1.23.5 pyarrow=14.0.2`
   followed by `SHA GATE PASS` with the anchor SHA. The failure came at
   SparkContext init: `java.io.FileNotFoundException: No such file or
   directory: s3a://YOUR_BUCKET/Access/synthetic/eventlog` — the event-log S3
   prefix did not exist yet and Spark does not create it. A setup miss,
   not a data or code defect; the prefix evidently existed by step 3.
   (Note for the runbook: the upload script should create the `eventlog/`
   prefix, or the job should tolerate its absence.)
3. **<STEP_ID> (step 3, 13:04–13:07 EST, exit 0, 228 s):**
   app 0002, the successful run. All 9 pipelines OK/SKIPPED-as-designed
   (F_A/F_B analytic-only per D4, as specified).

Health from the event log + verifier GT file:

- **0 failed tasks out of 5,036** across 47 stages. Zero task failures
  anywhere in the run.
- **Spill: exactly one task spilled** — J_N stage 38 task 3197, the
  max-load task itself: 536,870,912 B memory spill + 211,438,300 B disk
  spill (748,309,212 B total). The task succeeded; spill does not affect
  the record-count load (the GT load is shuffle-read RECORDS, and the
  J_N load matches its D8 cross-check to 0.0015%). All other pipelines:
  0 spill bytes. The spill is a genuine execution detail of the hottest
  join task, not a defect.
- **Gap:** the driver stdout for app 0002 was not delivered (its AM
  container has no `stdout.gz` in logs.zip). App 0001's stdout covers the
  same cluster's Phase 0 (numpy version + SHA gate print), and the
  manifest + event log are the primary evidence, so this does not block
  verification — but the successful run's own printout is missing from
  the archive.
- logs.zip also contains the OLD cluster <CLUSTER_ID> (no-op step
  02:36 EST exit 0; job step 02:40 EST exit 1 in 50 s — the v1 numpy-gate
  failure). It is not the cluster that produced this evidence.

## R2.6 Verifier gates (package's own `verify_emr_run.py`, exit 0)

Run with `--skip-download` on the delivered files; outputs in
`round2/emr_verify_out/` (`emr_ground_truth_100m.json`,
`verification_report.json`).

| Gate | Result | Detail |
|---|---|---|
| `counts_sha_manifest` | PASS | exact anchor match |
| `counts_sha_npz` | SKIP | counts.npz not delivered (see §R2.2) |
| `placement` | PASS | 18 distinct hosts |
| `all_9_pipelines` | PASS | 9/9 with event-log ground truth |
| `threshold_audit` | PASS | verdicts recompute 9/9 at T=2,000,000; 7/9 at T/10=200,000 (the Databricks 100M pattern — G_B/J_B flip, as expected) |
| `d8_cross_check` | PASS | max \|event-log / D8 − 1\| = 0.0098% |

## R2.7 Comparison vs Databricks 100M GT and local arms

Verdict-bearing loads are event-log max per-task shuffle-read records
(F pipelines: analytic splitmix64 from Spark-produced counts, D4).
Threshold T = 2,000,000 throughout. No 10M numbers appear anywhere here.

| Pipe | EMR GT load | Verdict | DBX GT load | EMR/DBX | D8 load | A5 Q | A8 Q | A1 Q | A3 Q |
|---|---|---|---|---|---|---|---|---|---|
| G_A | 10,873,845 | FAIL | 10,873,845 | 1.0000 | 10,873,845 | 1.1401 | 1.1401 | 1.1386 | 6.8466 |
| G_B | 1,571,791 | PASS | 1,571,791 | 1.0000 | 1,571,791 | 2.0347 | 2.0348 | 2.0286 | 6.0058 |
| G_N | 10,442,419 | FAIL | 10,442,419 | 1.0000 | 10,442,419 | 1.1399 | 1.1399 | 1.1427 | 6.8604 |
| J_A | 10,873,999 | FAIL | 10,873,845 | 1.0000 | 10,873,845 | 1.0368 | 1.0372 | 117.3031 | 6.7130 |
| J_B | 1,571,945 | PASS | 1,571,791 | 1.0001 | 1,571,791 | 1.0849 | 1.0893 | 33.3039 | 4.6730 |
| J_N | 10,442,573 | FAIL | 10,442,419 | 1.0000 | 10,442,419 | 1.0364 | 1.0369 | 121.5666 | 6.7274 |
| F_A | 54,690,910,911,173 | FAIL | 54,690,910,911,173 | 1.0000 | — (analytic) | 1.0000 | 1.0000 | 1.0046 | 36.1575 |
| F_B | 371,507,261,637 | FAIL | 371,507,261,637 | 1.0000 | — (analytic) | 1.0010 | 1.0007 | 1.0040 | 8.9346 |
| W_A | 10,873,845 | FAIL | 10,873,845 | 1.0000 | 10,873,845 | 1.1401 | 1.1401 | 1.1386 | 6.8466 |

**EMR vs Databricks 100M: verdicts 9/9.** Six loads are bit-identical; the
three J loads are +154 records each — the event-log shuffle-read counts the
dim side (154 dim records land on the max task) while D8 counts only the
repartitioned join output. Ratio 1.0000–1.0001; the D8 cross-check bounds
the difference at 0.0098%.

| Arm | EMR verdicts | EMR median Q | DBX-100M verdicts | DBX-100M median Q | Disagreements (EMR) |
|---|---|---|---|---|---|
| Local sim | 9/9 | 1.1208 | 9/9 | 1.1209 | — |
| A1 | 7/9 | 1.1427 | 7/9 | 1.1427 (identical) | J_A, J_N |
| A2 | 9/9 | 1.1405 | 9/9 | 1.1405 | — |
| A3 | 4/9 | 6.8466 | 4/9 | 6.8466 (identical) | G_A, G_N, J_A, J_N, W_A |
| A4 | 9/9 | 1.1445 | 9/9 | 1.1445 | — |
| A5 | 9/9 | 1.0849 | 9/9 | 1.0850 | — |
| A8 | 9/9 | 1.0893 | 9/9 | 1.0894 | — |

Every arm's EMR result matches its Databricks-100M result — verdicts,
disagreement sets, and medians to ≤0.01% (A1/A3 medians bit-identical; A5/A8/
local-sim medians differ only in the 4th decimal, from the +154 J loads).
A1's J_A/J_N misses are the known join-emergent failure mode, now measured
on a real multi-node engine (Q-errors 117×/122× on those pipelines). A5's
worst Q is G_B at 2.0347, same as the Databricks leg.

## R2.8 What is still missing, and whether it matters

| Missing item | Matters? |
|---|---|
| `data/counts.npz` + `counts_summary.json` + `meta.json` | No (verdict) — the manifest SHA + gate ordering + round-1 corpus checks + app-0001 gate print jointly settle dataset identity. Yes (completeness) — direct recomputation is still impossible; request the 3 small files for the archive if convenient. |
| Driver stdout for app 0002 | No — manifest + event log are the evidence; app-0001 stdout covers the same cluster's Phase 0. |
| EMR release label (literal string) | No for the paper's claims (Spark 3.5.8-amzn-0 is the precise version to cite). Do not write "emr-7.14.0" as a verified fact — it is the package's specification, not an observed value. |
| Executed bootstrap script + step-3 submit command | **Yes, for the reproduction package** — the executed setup exists only in S3 and in these logs. Archive `s3://YOUR_BUCKET/Access/synthetic/code/bootstrap_action.sh` as-run. Until then, the paper's reproduction package (v2.1) documents a setup route that reaches the same end state by a different, locally validated route — say so if asked, do not claim v2.1 ran. |
| User's own `emr_verify_out/` | No — the verifier was run here on the delivered files with the package's own script (exit 0); outputs are in `round2/emr_verify_out/`. |

## R2.9 Manuscript-ready statement

> Ground truth for the 100M-row zipf/s101 corpus was executed on a dedicated
> AWS EMR cluster (1 master + 19 core m5.xlarge nodes, YARN, Spark
> 3.5.8-amzn-0): 18 executors ran on 18 distinct hosts (driver on a 19th),
> proven from the Spark event log (`<APPLICATION_ID>`, 5,036
> tasks, 0 failed). The generated corpus passed the counts SHA-256 gate
> against the Databricks anchor (`3549c275…`). All 9 pipeline verdicts at
> T=2,000,000 agree with the Databricks 100M ground truth (9/9); loads agree
> to a ratio of 1.0000–1.0001 (J loads +154 records from the dim side of the
> shuffle). A5: 9/9 verdicts, median Q-error 1.085. A1 misses the two
> join-emergent failures (J_A, J_N; 7/9). F_A/F_B loads are analytic
> (splitmix64 from Spark-produced counts; the joins are not materialisable).
> One J_N task spilled 748 MB (memory + disk) and succeeded; no other
> pipeline spilled. The in-job topology probe failed on this Spark version
> (missing `statusTracker.getExecutorInfos`); the event log is the deciding
> audit trail. Setup note: the executed bootstrap built a venv with the
> pinned numpy 1.23.5 / pyarrow 14.0.2 and pointed PYSPARK_PYTHON at it
> (applied on the laptop, not the packaged v2.1 bootstrap — same pinned
> end state, verified on all 20 nodes).

**R2 item status: CLOSED.** The multi-node ground-truth run the R2 objection
demanded now exists, is verified end-to-end by the package's own verifier,
and agrees with the Databricks 100M ground truth on every verdict. The
remaining open items are archival (executed bootstrap script, counts.npz),
not evidential.
