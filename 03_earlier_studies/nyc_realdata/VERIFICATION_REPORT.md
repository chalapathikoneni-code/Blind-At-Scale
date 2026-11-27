# Real-Data (NYC TLC) 100M EMR Run — Verification Report

**Date:** 2026-11-24. **Verifier:** independent check of the delivered
`realdata_results.zip` against the packaged realdata arm
(`realdata_emr_package/`, v2.1) and the design
(`realdata_arm/REALDATA_ARM_DESIGN.md`). Every headline number below was
recomputed from the raw files, not copied from the verifier's report —
where the verifier's values are quoted, the recomputation is stated
alongside and agrees exactly.

## 0. Delivery

- Drive file `realdata_results.zip`, id `1XcTx_4VSzBNgBdjX-cZBn4y3sT3u2OKb`,
  38,246,748 B, md5 `5976e7588a159ae19ca787f2ffe38f27` — **download verified:
  size and md5 match the delivery notice exactly.**
- Zip integrity: `unzip -t` reports **"No errors detected"** — 4,313 files,
  409,079,505 B uncompressed. (Unlike the synthetic round-1 zip, this one is
  complete: central directory and EOCD present.)
- Contents vs the runbook's expectations:

| Expected | Present | Notes |
|---|---|---|
| `output/run_manifest.json` | yes (78,574 B) | 44 pipeline entries, stages C + S |
| `output/job_summary.json` | yes (14,090 B) | same 44 entries, D8 + analytic |
| `eventlog/<app-id>` | yes (195,344,514 B) | `<APPLICATION_ID>_1`, finalized |
| `data/dataset_manifest.json` | yes (52,473 B) | staged corpus manifest (see §3) |
| `config/` (predictions, analytic GT, manifest, fleet) | yes | predictions + analytic GT byte-identical to packaged v2.1 |
| `docs/` (runbook, validation report) | yes | **v1 docs, not v2.1** (see §7) |
| `realdata_verify_out/` (verifier outputs) | yes | `verification_report.json` (PASS) + `realdata_ground_truth.json` + copies of output/eventlog/data |
| Step + container + node logs | yes | **five clusters' logs** (see §1) |
| `counts.npz` / per-key counts files | no | Not part of this arm's outputs; the staging manifest + gate cover dataset identity (see §3). Not evidential. |

## 1. Provenance verdict: GO

**The run is verified end-to-end. The multi-node real-data ground truth is
real, placed on 18 hosts, and agrees with the design's predictions.**

The package's own verifier (`verify_realdata_run.py`, run by the operator —
its outputs are in the delivery) exits **PASS with 0 failures, all 8 gates
green**. Every gate was independently re-checked here from the raw files:

| Gate (verifier) | Verifier | Independent re-check |
|---|---|---|
| study_tag | PASS | All inputs `study=NYC_TLC_100M` (manifest, job summary, predictions CSV, analytic GT, ground truth) — confirmed |
| manifest_match | PASS | Delivered `data/dataset_manifest.json` vs shipped config manifest: 55/55 files, **0 SHA mismatches, 0 row-count mismatches, 0 size mismatches** (full-dict compare; only `staging_seconds` differs: 127.2 vs 512.1) |
| placement | PASS, 18 hosts | **My own event-log parse: 18 distinct executor hosts** (ExecutorAdded) = 18 task hosts, same set; 25,377 task ends, 0 failed, 305 stages, ApplicationEnd present |
| all_pipelines_have_gt | PASS, 44/44 | 44 manifest entries all carry ground truth in `realdata_ground_truth.json` — confirmed |
| threshold_audit | PASS, D8 32/32, analytic 44/44 | Recomputed from the manifest's loads vs thresholds: **D8 32/32, analytic 44/44** — exact |
| d8_cross_check | PASS, max 0.0205% | Recomputed from ground-truth entries: **max \|event-log/D8 − 1\| = 0.0205%** (at C/2026-02/J_N), n=32 — exact |
| analytic_gt_match | PASS, 44/44 | Manifest analytic loads vs shipped `analytic_gt_calibration.json`: **0 mismatches** (loads and both thresholds) |
| analytic_f_gate | PASS, 6/6 | Stage C F entries carry `analytic_gate: PASS` on all 6 — confirmed |

Arm comparison independently recomputed from `predictions_realdata.csv` +
executed loads: **all 9 arms match the verifier's agreement counts and
median Q-errors to 4 decimals** (§6).

## 2. What actually ran — cluster and step history (read this first)

**The successful run is on cluster `<CLUSTER_ID>` — NOT
`<CLUSTER_ID>` as briefed.** The task brief assumed the pre-staged
rerun would use the kept j-0754 cluster; the evidence shows a new cluster
was created for it (bootstrap 2026-11-24 03:32:37–03:32:45 UTC) and the job
ran there. The j-0754 cluster never ran a successful job. The zip contains
logs for five clusters; their fates:

| Cluster | Nodes | Outcome |
|---|---|---|
| <CLUSTER_ID> | 20 | Bootstrap FAILED on all nodes, 2026-11-23 07:47 UTC (the v1 pin failure: numpy 2.5.5 / pyarrow 18.0.2 — already known) |
| <CLUSTER_ID> | 20 | Bootstrap FAILED on all nodes, 2026-11-23 07:40 UTC (same v1 failure) |
| <CLUSTER_ID> | 20 | Bootstrap FAILED on all nodes, 2026-11-23 15:45 UTC (bootstrap script exit 1) |
| <CLUSTER_ID> | 20 | Bootstrap OK; job step s-0281 FAILED exit 1 after 126 s (see below) |
| **<CLUSTER_ID>** | **20** | **Bootstrap OK; step s-0542 SUCCEEDED exit 0, 640 s** |

Step history on j-0268 (all times UTC 2026-11-24):

| Step | Time | Outcome |
|---|---|---|
| <STEP_ID> | 03:35:35 | Succeeded, 2 s (setup/no-op) |
| <STEP_ID> | 03:36–03:38:55 | **FAILED exit 1, 130 s** — app `..._0001`: AM container exit 13, `TimeoutException: Futures timed out after [100000 milliseconds]`. The driver had started Phase 0 and verified ~13 files from S3 when the AM died. |
| <STEP_ID> | 03:42:08 | FAILED exit 255, 4 s — a `yarn logs` fetch with a missing `applicationId` argument (operator error, not a job failure) |
| **<STEP_ID>** | **03:44:58–03:55:38** | **SUCCEEDED exit 0, 640 s** — app `<APPLICATION_ID>`, both stages C + S, all 44 entries |

**The `_0001` failure mode (both clusters).** The j-0754 step s-0281 died
with the byte-identical signature: AM container exit 13, `TimeoutException:
Futures timed out after [100000 milliseconds]`, after 126 s. In both cases
the driver was mid-Phase-0 (staging) when the ApplicationMaster's 100 s
future expired. The retry on j-0268 ran the identical 127 s staging and
succeeded, so the timeout is a startup race, not a data or code defect. The
mechanism is not definitively root-caused from these logs; it is consistent
with an AM startup race and is reported as such.

**Correction to the earlier diagnosis of the j-0754 failure.** The standing
record says j-0754's job failed because the subnet had no outbound internet
and the CloudFront download died ~3 min in. **The delivered logs do not
support that for the attempt they cover.** The j-0754 app `_0001` driver
stdout (97 lines) shows the fresh-download path working: ~40 files
downloaded from CloudFront with per-file SHA-256 verification passing
(all green months + yellow through 2026-01, ~1.5 GB) before the stdout ends
abruptly at the AM timeout. CloudFront was reachable from that driver. The
step died on the AM timeout, not on a network error (no connection exception
anywhere in the stderr). Boundary: an earlier j-0754 attempt (the ~10:30 EST
one in the record) has no logs in this delivery, so nothing here speaks to
it; the correction covers the 13:15–13:19 EST attempt whose logs were
delivered.

## 3. Placement

From my own streaming parse of `realdata/eventlog/<APPLICATION_ID>_1`
(195 MB, ApplicationEnd present — finalized):

- **18 distinct executor hosts** (`SparkListenerExecutorAdded`), identical
  to the 18 distinct task hosts (`SparkListenerTaskEnd`) — same set both
  ways. (Hostnames recorded in the verifier's report file only.)
- 18 executors × 4 cores each (executor IDs 1–18 in the event log).
- 25,377 task ends, **0 failed tasks**, 305 stages.
- Driver on a 19th core host (manifest `driver_hostname`); master is the
  20th node. Cluster shape per `config/fleet.json`: 1 master + 19 core,
  m5.xlarge (m6i.xlarge alternate), 64 GB gp3, flexible fleet — the package
  spec.
- The in-job topology probe failed on this Spark version, exactly as in the
  synthetic run (`statusTracker` has no `getExecutorInfos`; `memStatus`
  TypeError) — at both the start and end probes. The event log is the
  deciding audit trail, as designed.

## 4. Staging gate (pre-staged mode)

- **Mode: pre-staged.** The driver stdout for app `_0002` shows every file
  fetched `from S3` (no CloudFront download on the cluster) and ends:
  `STAGING GATE PASS: 55 files, 103,139,628 yellow + 1,295,576 green rows,
  1.79 GB, 127s`. Phase 0 total 143 s. Data path
  `s3a://YOUR_BUCKET/Access/realdata/data/nyc` — the intended pre-staged prefix.
- **Exact corpus: 103,139,628 yellow + 1,295,576 green rows, 55 files,
  1,791,549,940 bytes** — matches the design's staging-gate expectation
  exactly, and the delivered manifest matches the shipped manifest on all
  55 files (0 SHA / row-count / size mismatches; §1). Spot SHA prefixes in
  the driver log match the design's known values (lookup `1a99e105…`,
  green 2026-01 `b0b188cd…`, yellow 2026-01 `8b3933fe…`, yellow 2024-07
  `af12d6b0…`). Zone lookup: 265 zones, IDs 1..265 confirmed.
- **One version signal:** the packaged v2.1 job writes a `staging_mode`
  field (`"pre-staged-s3"` / `"cloudfront"`) into the re-uploaded manifest,
  but **neither the delivered `data/dataset_manifest.json` nor the run
  manifest records any `staging_mode` value**, and the run manifest's
  `dataset_manifest_sha256` is `None`. The pre-staged mode is proven
  behaviorally (stdout + S3 paths + 127 s staging), not by a recorded flag
  — and the missing flag is one of three signs the running job file is not
  the packaged v2.1 file (see §7).

## 5. Study tag and environment

- **Study tag: `NYC_TLC_100M`** on every output (run manifest, job summary,
  predictions CSV — all 396 rows asserted in the recomputation — analytic
  GT file, dataset manifest, ground-truth JSON). No synthetic-study file is
  referenced anywhere; the verifier's tag gate passed.
- **Environment fingerprint** (each value with its source):
  - Spark **3.5.8-amzn-0** (manifest, job summary, ground truth; event-log
    jar paths confirm `spark-core_2.12-3.5.8-amzn-0.jar`)
  - YARN, 128 shuffle partitions (manifest)
  - Python **3.11.18** (manifest environment block; bootstrap log)
  - **numpy 1.26.4, pyarrow 18.0.0, xxhash 4.1.0** (bootstrap VERIFIED line,
    all 20 nodes — see §7; note numpy is 1.26.4, not the v2.1 pin 2.0.2)
  - AL2023, Linux 6.1.190-237.431.amzn2023, glibc 2.34 (manifest)
  - Cluster **<CLUSTER_ID>**, step **<STEP_ID>**,
    app **<APPLICATION_ID>**
  - Successful step 2026-11-24 03:44:58–03:55:38 UTC (640 s); pipeline span
    448.1 s; Phase 0 staging 127.2 s
- **No EMR release label is recorded anywhere in the delivered logs.**
  The manifest's `emr_release_label` field holds a 200-character truncated
  JSON fragment (master host + bootstrap script path, cut off mid-object)
  containing no release string. **Do not cite "emr-7.14.0" as a verified
  fact** — cite Spark 3.5.8-amzn-0 only. (Same lesson as the synthetic run.)

## 6. Job health

- Step states (§2): 1 setup step OK, 1 job attempt failed on the AM
  timeout, 1 operator-errored log fetch, 1 job attempt **succeeded**.
- Pipeline statuses: **36 OK + 8 SKIPPED_NOT_MATERIALISABLE** (F_A/F_B × 4
  instances — analytic-only by design, D4).
- **0 failed tasks** across 25,377 task ends.
- **Spill: 4 tasks spilled, 4,410,001,438 bytes total (~4.41 GB)** — the
  only resource event in the run. Attributed by stage-submit time to
  pipeline windows: stage 298 (2,517,426,184 B over 2 tasks) inside **Stage
  S J_A**, stage 308 (1,892,575,254 B over 2 tasks) inside **Stage S J_B**.
  Both are `collect` stages (`collect at spark_job_realdata.py:266`) in
  the 100M fact–dimension joins; the tasks succeeded and the loads are
  unaffected (the D8 cross-check passes at 0.0205%, and the verdict-bearing
  loads come from shuffle-read metrics, not the collects). This is a real
  observed resource event at 100M scale, not a failure.
- **Wall time is skew-dominated:** Stage S G_N took **203.0 s** of the
  439.7 s total pipeline wall time (46%) — the RatecodeID NULL mass (~30M
  rows) lands on one partition and one task does that work. Next longest:
  Stage S J_A 32.4 s, G_A 27.4 s. All Stage C pipelines ≤ 13.8 s.
- Two non-blocking warnings in the driver log: (a) a PySpark
  `RuntimeWarning` that `realdata_model.py` could not be added to the
  Python path via `spark.submit.pyFiles` (the driver imports it directly;
  all analytic values match the shipped GT exactly, so the warning had no
  effect on results); (b) the known topology-probe failure (§3).

## 7. Package version that ran: a laptop hand-fix hybrid, NOT v2.1

Three independent signs show the executed setup is not the packaged v2.1
tree. The run is valid (all gates pass; the comparison inputs are
byte-identical to packaged v2.1), but **the reproduction package does not
contain the exact setup that ran** — the same archival gap as the synthetic
run, now for the realdata arm too.

1. **Bootstrap: laptop hand-fix venv (all 20 nodes).** Every node's
   bootstrap stdout reads: `bootstrap: building venv /usr/local/bas-venv
   from python3.11` → PyPI installs → `VERIFIED
   /usr/local/bas-venv/bin/python: numpy=1.26.4 pyarrow=18.0.0 xxhash(pip
   imported ok)` → `BOOTSTRAP OK: venv /usr/local/bas-venv has pinned
   numpy+pyarrow+xxhash (PYSPARK_PYTHON points here).` All 20 bootstrap
   controllers report `Execution succeeded`. v2.1 markers across all 20
   nodes: `HADOOP-VERIFIED` **0**, `break-system-packages` **0**, vendored
   wheels **0**; `bas-venv` mentioned 100×. The numpy pin is **1.26.4**
   (Claude's hand fix from the v1 failure), not v2.1's 2.0.2. For this arm
   numpy is not result-affecting (predictions precomputed; analytic loads
   are integer arithmetic) — and the evidence proves it: the analytic loads
   computed on-cluster with numpy 1.26.4 match the shipped GT (computed
   with numpy 2.5.5 in the dry-run venv) exactly on all 44 entries.
2. **Docs: v1.** The `docs/` in the delivery are the v1 runbook/validation
   report (they describe `realdata_emr_package.zip` with job-side
   CloudFront download; no vendored wheels, no pre-staged section), while
   the packaged tree is v2.1 (offline zip, `code/wheels/`, pre-staged
   mode). SHA-256 differs on both files.
3. **Job file: not v2.1.** The event log's spilling stages are named
   `collect at spark_job_realdata.py:266`; in the packaged v2.1 file line
   266 is staging code and the collects are at lines 300/463/565/629/704/
   714 — the running file is ~34 lines shorter at that point. Together
   with the missing `staging_mode` recording (§4), the running job file is
   a different version from the packaged v2.1 file (it has pre-staged
   fetching, so it is not pure v1 either — most plausibly v2 or a
   hand-edited hybrid; the exact file lives only in S3
   `s3://YOUR_BUCKET/Access/realdata/code/`).

**Archival ask (not evidential):** the as-run `spark_job_realdata.py`,
`realdata_model.py`, `stage_data.py`, and `bootstrap_action.sh` from S3
should be pulled into the reproduction package so the executed setup is
preserved. The comparison inputs need no action: `predictions_realdata.csv`
and `analytic_gt_calibration.json` in the delivery are byte-identical to
the packaged v2.1 files (SHA-256 verified).

## 8. Stage C — calibration: executed-vs-analytic gap (recomputed)

The gap is a result, not a bug: Spark partitions with Murmur3, the
partner's analytic model with xxhash64+splitmix64. All loads below are the
verifier's event-log ground truth, recomputed here from
`realdata_ground_truth.json`.

| Instance / pipeline | Executed | Analytic | Ratio |
|---|---|---|---|
| 2026-01 G_A | 207,031 | 199,975 | 1.0353 |
| 2026-01 G_B | 168,975 | 191,226 | 0.8836 |
| 2026-01 G_C | 10,547 | 10,596 | 0.9954 |
| 2026-01 G_N | 2,390,495 | 2,390,495 | 1.0000 |
| 2026-01 J_A | 207,036 | 199,975 | 1.0353 |
| 2026-01 J_B | 168,978 | 191,226 | 0.8837 |
| 2026-01 J_N | 10,549 | 10,596 | 0.9956 |
| 2026-01 W_A | 207,031 | 199,975 | 1.0353 |
| 2026-01 F_A / F_B | 173,798,004 / 330,477,337 | same (analytic by construction) | 1.0000 |
| 2026-02 G_A | 178,114 | 187,622 | 0.9493 |
| 2026-02 G_B | 153,599 | 179,135 | 0.8574 |
| 2026-02 G_C | 9,733 | 9,772 | 0.9960 |
| 2026-02 G_N | 2,162,640 | 2,162,640 | 1.0000 |
| 2026-02 J_A | 178,117 | 187,622 | 0.9493 |
| 2026-02 J_B | 153,602 | 179,135 | 0.8575 |
| 2026-02 J_N | 9,735 | 9,772 | 0.9962 |
| 2026-02 W_A | 178,114 | 187,622 | 0.9493 |
| 2026-02 F_A / F_B | 148,623,071 / 257,451,181 | same | 1.0000 |
| 2026-03 G_A | 212,796 | 215,595 | 0.9870 |
| 2026-03 G_B | 181,924 | 205,013 | 0.8874 |
| 2026-03 G_C | 11,257 | 11,295 | 0.9966 |
| 2026-03 G_N | 2,719,036 | 2,719,036 | 1.0000 |
| 2026-03 J_A | 212,801 | 215,595 | 0.9870 |
| 2026-03 J_B | 181,927 | 205,013 | 0.8874 |
| 2026-03 J_N | 11,259 | 11,295 | 0.9968 |
| 2026-03 W_A | 212,796 | 215,595 | 0.9870 |
| 2026-03 F_A / F_B | 187,374,735 / 359,123,568 | same | 1.0000 |

- **Median ratio 0.9961** (all 30 scored entries); non-F median 0.9912;
  range **0.8574–1.0353**.
- Pattern: G_B is systematically low in all three months (0.857–0.887 —
  Murmur3 spreads the dropoff-zone mass more evenly than the analytic model
  predicts); G_A varies by month (0.949–1.035); G_C ≈ 0.996; G_N is exact
  (1.0000 — the NULL mass is a single key, partition-independent). J
  pipelines track their G counterparts to within +2–5 records (the dim
  side of the shuffle).
- **Verdicts agree 30/30 under both thresholds** — but see the saturation
  caveat in §9: all 30 are FAIL, so the agreement is uninformative as
  discrimination; the loads are the finding.
- The design's expectation is met: the 2026-01 analytic loads reproduce
  the partner's values exactly (G_A 199,975; G_B 191,226; G_C 10,596;
  G_N 2,390,495; F_A 173,798,004; F_B 330,477,337), and the executed loads
  sit within ±15% of them.

## 9. Stage S — 100M scale (recomputed)

Thresholds (from the manifest; recomputed and exact): relative primary
t_rel = 1,611,557 (2 × mean; design estimate ≈1.59M); absolute secondary
t_abs = 2,062,793 (0.02 × 103,139,628 = 2,062,792.56). For G_C/J_N (green
table): t_rel = 20,243, t_abs = 25,912. F thresholds are per-pipeline
(t_rel 8.16B / 22.23B; t_abs 2,062,793).

| Pipeline | Executed load | t_rel | t_abs | Verdict rel | Verdict abs | Source |
|---|---|---|---|---|---|---|
| G_A | 5,997,032 | 1,611,557 | 2,062,793 | FAIL | FAIL | event log shuffle-read |
| G_B | 4,758,481 | 1,611,557 | 2,062,793 | FAIL | FAIL | event log shuffle-read |
| G_C | 319,783 | 20,243 | 25,912 | FAIL | FAIL | event log shuffle-read |
| G_N | 74,261,964 | 1,611,557 | 2,062,793 | FAIL | FAIL | event log shuffle-read |
| J_A | 5,997,037 | 1,611,557 | 2,062,793 | FAIL | FAIL | event log shuffle-read |
| J_B | 4,758,484 | 1,611,557 | 2,062,793 | FAIL | FAIL | event log shuffle-read |
| J_N | 319,785 | 20,243 | 25,912 | FAIL | FAIL | event log shuffle-read |
| W_A | 5,997,032 | 1,611,557 | 2,062,793 | FAIL | FAIL | event log shuffle-read |
| F_A | 134,525,346,872 | 8,164,866,604 | 2,062,793 | FAIL | FAIL | **analytic only (D4)** |
| F_B | 278,665,174,202 | 22,225,904,949 | 2,062,793 | FAIL | FAIL | **analytic only (D4)** |
| D_A | 262 | 50,000 | 50,000 | PASS | PASS | Spark distinct (diagnostic, unscored) |

- **Stage S verdicts: 10/10 FAIL under the relative threshold, 10/10 FAIL
  under the absolute threshold; relative-vs-absolute disagreements: 0.**
- Executed-vs-analytic at 100M: G_A 5,997,032 vs analytic 5,846,613
  (ratio 1.0257); G_N 74,261,964 — exactly the design's V2 dry-run value;
  F_A/F_B analytic values exactly the design's V2 values
  (134,525,346,872 / 278,665,174,202).
- J-minus-G deltas are +2–5 records in every instance (the 265-row dim
  side of the shuffle) — the SHUFFLE_MERGE hint worked; the joins did not
  broadcast.

### Arms vs executed loads (recomputed from predictions_realdata.csv)

**Label discipline: these are the REAL-WORLD study's arms (his vocabulary),
not the synthetic study's.** RW-A1 = contribution-map sampling; RW-A5 =
sampled-tail estimator; RW-A8 = direct analytic prediction; RW-A2 =
hash-key sampling; RW-A3 = random-key sampling; B0/B1/B2 = top-k-only /
uniform-tail / Count-Min Sketch controls. 40 scored entries (4 instances ×
10 pipelines; D_A excluded).

| Arm (RW) | Verdicts rel | Verdicts abs | Median Q | Mean Q | Max Q | Infinite-Q |
|---|---|---|---|---|---|---|
| A1 | 40/40 | 40/40 | 1.0293 | 1.0524 | 1.1987 | 0 |
| A2 | **36/40** | **36/40** | 7.7031 | 6.8579 | 9.3281 | 4 |
| A3 | 40/40 | 40/40 | 4.9549 | 5.6687 | 31.0392 | 0 |
| A4 | 40/40 | 40/40 | 1.2096 | 1.6415 | 7.7126 | 0 |
| A5 | 40/40 | 40/40 | **1.0098** | 1.0397 | 1.1682 | 0 |
| A8 | 40/40 | 40/40 | **1.0055** | 1.0409 | 1.2462 | 0 |
| B0_TOPK_ONLY | 40/40 | 40/40 | 1.0076 | 1.0349 | 1.1435 | 0 |
| B1_UNIFORM_TAIL | 40/40 | 40/40 | 1.0104 | 1.0396 | 1.1826 | 0 |
| B2_COUNT_MIN_SKETCH | 40/40 | 40/40 | 1.0092 | 1.0398 | 1.1663 | 0 |

- **A5 vs A1 (RW):** A5 median Q 1.0098 vs A1 1.0293 overall — A5 better,
  but both 40/40 on verdicts. Per instance: A5 beats A1 in all three
  calibration months (Jan 1.0201 vs 1.0412; Feb 1.0269 vs 1.0495; Mar
  1.0098 vs 1.0268); **at 100M they tie for practical purposes — A1
  1.0146 vs A5 1.0153 (A1 marginally better)**. A8 has the best overall
  median (1.0055) and the best at 100M (1.0055). B0/B1/B2 cluster with A5
  (≈1.008–1.010) — the head-dominated NYC data makes several estimators
  nearly equivalent on load fidelity.
- **A2's failure mode:** predicts **0** on G_N in all 4 instances (infinite
  Q, verdict PASS vs executed FAIL) — hash-key sampling drops the NULL
  mass entirely (RatecodeID NULLs are ~29% of rows; G_N's executed load is
  the NULL partition). The only verdict disagreements in the whole arm
  comparison.
- **A3:** verdicts right (40/40) but magnitudes badly off (median Q 4.95,
  max 31.04) — random-key sampling preserves the FAIL call on this
  high-skew data but not the loads.

### Saturation caveat (read before citing any verdict figure)

**All 40 scored executed verdicts are FAIL under both thresholds (40/40
FAIL, 0 PASS).** The only PASS entries are the 4 D_A diagnostics, which are
excluded from scoring. Verdict agreement is therefore non-discriminating in
this arm: any method that predicts FAIL everywhere scores 40/40, and recall
is uninformative. The result that separates the arms is **load fidelity
(Q-error)**, not verdicts — and on that metric the ordering is A8 ≈ B0 ≈
B2 ≈ A5 ≈ B1 (≈1.006–1.010) > A1 (1.029) > A4 (1.210) > A3 (4.955) >
A2 (7.703, plus 4 verdict misses). The manuscript must report the
verdicts with this caveat, not as a discrimination result. This is the
same saturation concern the pre-submission review raised (P1.1); it
applies to this arm as executed.

## 10. What is still missing, and whether it matters

| Missing item | Matters? |
|---|---|
| As-run code (`spark_job_realdata.py`, `realdata_model.py`, `stage_data.py`, `bootstrap_action.sh`) from S3 | **Archival only.** The results are valid without it, but the reproduction package cannot claim to contain the executed setup until it is archived (§7). Request from the operator. |
| EMR release label | **No.** Cite Spark 3.5.8-amzn-0; the release string is not recorded (§5). |
| Per-key counts files (`counts.npz`-style) | **No.** Not part of this arm's design; dataset identity rests on the staging manifest + gate (§4), both verified. |
| Logs for the ~10:30 EST j-0754 attempt | **No, for the verdict.** Relevant only to the history of the "no outbound internet" diagnosis (§2); the successful run's evidence is complete without it. |
| j-0754's fate (still running? terminated?) | **Billing only.** The cluster never ran the successful job; whether it still exists is an operator question, not an evidence question. |
| A held-out PASS instance for the RW arms | **Scientific, not evidential.** The 40/40-FAIL saturation (§9) limits what verdict agreement can show. A future workload with passing instances would discriminate the arms on verdicts; nothing in this delivery provides one. |

## 11. Manuscript-ready statement

> Real-data ground truth was executed on a dedicated AWS EMR cluster (1
> master + 19 core m5.xlarge nodes, YARN, Spark 3.5.8-amzn-0): 18 executors
> ran on 18 distinct hosts (driver on a 19th), proven from the Spark event
> log (<APPLICATION_ID>, 25,377 tasks, 0 failed). The corpus
> is the NYC Taxi & Limousine Commission trip record data, 27 months
> (2024-07–2026-09): 103,139,628 yellow + 1,295,576 green rows across 55
> files (1,791,549,940 bytes), verified file-by-file against the shipped
> manifest (SHA-256, footer row counts, columns, key ranges) in
> pre-staged mode from S3 — no download on the cluster. Stage C
> (calibration, 2026-01–03 × 11 pipelines) measures the executed-vs-
> analytic gap: median executed/analytic load ratio 0.996 (range
> 0.857–1.035 over the 30 scored entries; G_B systematically low at
> 0.857–0.887, G_N exact at 1.000), with verdicts agreeing 30/30 under
> both thresholds. Stage S (100M corpus × 11 pipelines) finds all 10
> scored pipelines FAIL under both the relative (2× mean) and absolute
> (0.02×N) thresholds (0 disagreements); the largest executed load is G_N
> at 74,261,964 rows, and the Stage S G_N pipeline alone took 203 s of
> 440 s total pipeline time. F_A/F_B loads are analytic (splitmix64 from
> Spark-produced counts; the joins are not materialisable at 134.5B /
> 278.7B rows). Against the executed loads, the real-world study's arms
> score (40 scored entries): A5 median Q-error 1.010, A8 1.006, A1 1.029,
> A4 1.210, A3 4.955, A2 7.703 (36/40 verdicts — it predicts 0 on G_N in
> all 4 instances, missing the NULL mass); at 100M alone, A1 (1.015) and
> A5 (1.015) tie for practical purposes. **All 40 scored executed verdicts
> are FAIL, so verdict agreement does not discriminate the arms in this
> arm; load fidelity (Q-error) is the separating metric.** Four tasks
> spilled 4.41 GB in total in the Stage S J_A/J_B collect stages
> (2.52 GB + 1.89 GB); all tasks succeeded and no verdict-bearing load
> was affected. Setup note: the executed bootstrap built a venv with
> numpy 1.26.4 / pyarrow 18.0.0 / xxhash 4.1.0 from PyPI and pointed
> PYSPARK_PYTHON at it (applied on the laptop, not the packaged v2.1
> bootstrap — the analytic loads computed with it match the shipped
> ground truth exactly on all 44 entries). The first job attempt on the
> cluster failed on an ApplicationMaster startup timeout (100 s future)
> during staging; the retry succeeded. The in-job topology probe failed
> on this Spark version (missing statusTracker.getExecutorInfos); the
> event log is the deciding audit trail.
