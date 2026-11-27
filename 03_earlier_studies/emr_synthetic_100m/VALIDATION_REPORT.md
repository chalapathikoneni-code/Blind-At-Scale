# Blind at Scale — EMR 100M Package: Validation Report

**Package:** `emr_package.zip` (built 2026-11-23). **Scope:** 100M rows,
one run, ~20-node EMR cluster (1 master + 19 core, m5.xlarge).
**Anchor:** the Databricks D-SCALE 100M leg (Nov 22–23):
fact 100,000,000 / fact2 60,000,000 / dim 20,000 keys, zipf seed 101,
T = 2,000,000, counts SHA-256
`3549c275165e4e75c7de2e74c1b8d912cba4ee54141d7ff8db3abc36cd11dd8f`.

**Headline:** every check below passed. Two real defects were caught
and fixed during validation (a wrong numpy pin that would have failed
the SHA gate on the cluster, and a shell-quoting warning in the
cluster script) — both are recorded here with their evidence. The
Spark job itself CANNOT execute in this sandbox (JVM TCP is blocked
— see "What was NOT validated"), so the dry run covers everything up
to Spark execution, and the on-EMR safety nets (numpy gate, SHA gate,
placement gate) are designed to fail LOUD rather than produce wrong
results.

---

## D1 — Full 100M dry run (the required deliverable): PASS

**What ran.** The package's own `code/spark_job.py` Phase 0
(`generate_and_upload`), executed locally under the EMR pin
combination (Python 3.11.18, numpy==1.23.5, pyarrow==14.0.2 — the
exact versions the bootstrap action installs), with S3 uploads
stubbed to a local directory. Two runs:

| Check | Result |
|---|---|
| Counts SHA-256 vs Databricks 100M anchor | **EXACT**: `3549c275…dd8f` |
| All 7 maxima (kA 9,537,885; kB 772,471; kC 5,281; kN 9,161,039; fact2 kA 5,726,027; kB 463,727; nulls 4,000,000) | all exact |
| `headline_expected_check` | PASS |
| fact.parquet rows | 100,000,000 (100 row groups, 1,497.8 MB) |
| fact2.parquet rows | 60,000,000 (60 row groups, 234.9 MB) |
| dim.parquet rows | 20,000 |
| counts.npz SHA recomputed from the written file | exact |
| Generation time (this VM, 1.5 shared vCPU) | 179.5 s first run, 72.9 s rerun (Databricks: 57.9 s) |
| Peak RSS, 100M run | **2.92 GB** (single-process rerun; the 8g driver setting has ~2.7x headroom) |

**The numpy landmine (caught here, not on the cluster).** The
original brief said "pinned numpy 2.5.4". A counts-only replay at
100M under numpy 2.5.4 gives SHA `fd25cafc…` — DIFFERENT from the
anchor (fact kN max 9,162,759 vs 9,161,039; fact2 kB max 464,514
vs 463,727). numpy's `Generator.multinomial` changed between 1.23
and 2.x. The 100M anchor was generated under numpy 1.23.5, so the
package pins **numpy==1.23.5 exactly**, the job aborts on any other
version, and the SHA gate is the final arbiter. (The 10M anchor
`f9f3fa8b…` was generated under 2.5.4 — the two scales have
different anchors because they were generated under different
numpy versions. The 10M code-path test below confirms the 10M
anchor under 2.5.4.)

**10M code-path test: PASS.** The same generator code at 10M/6M
under numpy 2.5.4 reproduces the 10M anchor SHA `f9f3fa8b…`
exactly, with exact parquet row counts (10M / 6M / 20k) and peak
RSS 0.39 GB. (The job's numpy gate was relaxed in the test harness
only for this run — the packaged job still enforces 1.23.5.)

**Dry-run incident log (both fixed during validation).**
1. First 100M attempt died mid-write with `ENOSPC`: this VM's
   `/tmp` is a 512 MB tmpfs and the 100M parquet is ~1.8 GB.
   Fixed by pointing the harness at the big disk. Lesson
   recorded for the run: the driver's local staging needs ~2 GB
   free — the fleet's 64 GB EBS volumes cover it 30x over.
2. The harness itself had a bug (`ParquetFile.path` does not
   exist in pyarrow 14) that crashed the post-generation checks
   after a successful run; fixed, and the written files were
   verified directly (row counts + SHA above). The package code
   was not at fault.
3. A first rerun of the 100M dry run was killed by the sandbox
   at 143 s (exit 120, no traceback) while other sessions' jobs
   were running concurrently — sandbox memory pressure, not a
   package defect (the same code had already completed once).
   The second rerun passed end-to-end in a single process
   (SHA exact, row counts exact, peak RSS 2.92 GB).

## D2 — Verify/collect logic vs Databricks 100M ground truth: PASS

`cloud_program/shared/collect_cloud.py` re-run against the
Databricks 100M GT JSON + 100M predictions reproduces the recorded
`cluster_comparison_databricks_100m.json` exactly: threshold audit
9/9 at T=2,000,000 and 7/9 at 200,000; local sim 9/9, median Q
1.1209; A5 9/9, median Q 1.085; A1 7/9 (J_A, J_N); A3 4/9.
`verify_emr_run.py` reuses the same comparison logic (q-error,
verdict, threshold-audit definitions copied from
`collect_cloud.py` / `cluster_compare.py`).

## V1 — Counts replay (numpy-version matrix): PASS (with the pin change)

| numpy version | 100M counts SHA | vs anchor |
|---|---|---|
| 1.23.5 (EMR pin) | `3549c275…dd8f` | EXACT (all 8 checks) |
| 2.5.4 | `fd25cafc…` | MISMATCH (3 of 8 checks fail) |

This is the evidence for the 1.23.5 pin (see D1).

## V2 — Threshold audit reproduction: PASS

Covered by D2: 9/9 at T=2,000,000, 7/9 at T/10=200,000, matching
the Databricks 100M record exactly.

## V3 — Spark 3.5 API audit of `spark_job.py`: PASS

Every Spark API used was listed and checked against Spark 3.5.
All are long-stable public APIs; all except the D8 additions were
executed by the pod run on Spark 3.5.3, and the D8 additions were
executed by the Databricks run on Spark 4.2.0 (their Spark 3.5
existence is documented in the table).

| API | Since | Proven by |
|---|---|---|
| `SparkSession.builder.config().getOrCreate()` | 2.0 | pod 3.5.3 |
| `sparkContext.master / .applicationId / .setLogLevel` | 1.0 | pod 3.5.3 |
| `sparkContext.statusTracker().getExecutorInfos()` | 1.4 | Databricks 4.2.0 (dead under Spark Connect; works on YARN) |
| `sparkContext._jsc.sc().getExecutorMemoryStatus()` (fallback only) | 1.0 (Java API) | Databricks 4.2.0 fallback path |
| `spark.read.parquet`, `DataFrame.count/collect` | 1.3 / 1.0 | pod 3.5.3 |
| `repartition`, `persist` / `unpersist` (`MEMORY_AND_DISK`) | 1.0 | D8 (Databricks 4.2.0) |
| `groupBy / agg / join / hint("SHUFFLE_MERGE") / withColumn` | 1.3 / 3.0 (hint) | pod 3.5.3 |
| `F.col / F.count / F.max / F.row_number / F.spark_partition_id` | 1.3 / 1.6 (`spark_partition_id`) | pod 3.5.3 / D8 |
| `Window.partitionBy().orderBy()` | 1.4 | pod 3.5.3 |
| `spark.catalog.clearCache()` | 2.0 | D8 (Databricks 4.2.0) |
| `spark.stop()`, `spark.version` | 1.0 | pod 3.5.3 |
| Configs: `shuffle.partitions`, `maxPartitionBytes`, `autoBroadcastJoinThreshold`, `adaptive.enabled`, `eventLog.enabled/dir` | — | pod 3.5.3 |

**Databricks-only artefacts: NONE.** Grep for `spark.databricks.*`,
`/Volumes`, `photon` finds only two comment/docstring mentions
(the docstring stating they are NOT used, and a source-attribution
comment). No `spark.databricks.photon.enabled` config, no Unity
Catalog paths, no Spark Connect assumptions. `spark.sql.*` configs
set in the job match the pod's exactly; `dynamicAllocation` and
`maxAppAttempts` are set at submit time in `add_job_step.sh`.

**API differences between the two proven runs, resolved:** the
Databricks script's Photon config (`spark.databricks.photon.enabled`)
and its volume paths are dropped (EMR has no Photon, no Unity
Catalog); the Databricks script's `spark.catalog.clearCache()` and
`persist()` calls are kept (they failed on Databricks only because
of Spark Connect, and are standard Spark 3.5 APIs on YARN); the
pod's event-log measurement is restored as verdict-bearing (the
Databricks D8 measurement is kept as an in-job cross-check only).

## V4 — Static checks: PASS

- `python3 -m py_compile`: `code/spark_job.py`,
  `scripts/verify_emr_run.py` — clean. (One real syntax error was
  caught and fixed during this check: an unmatched `]` in an
  f-string in `verify_emr_run.py`.)
- `bash -n`: all four shell scripts — clean.
- `shellcheck` 0.11.0 (warnings level): all four shell scripts —
  clean. (One real warning was caught and fixed: SC2140 awkward
  quoting in the `--bootstrap-actions` argument of
  `emr_create_cluster.sh` — now a single quoted
  `Path=...,Name=...` value.)
- JSON validity: `fleet.json`, `instance_groups.json`,
  `summary_100m.json`, `dbx_gt_100m.json` — all parse.

## V5 — AWS CLI command construction: PASS (no live AWS call made)

`aws emr create-cluster` is a customised CLI command that does not
support `--generate-cli-skeleton`, so validation used three
offline methods (AWS CLI v1.46.3, botocore 1.43.62, installed
locally for this check):

1. **Flag parsing (real CLI, no network).** The full
   `create-cluster` flag set from `emr_create_cluster.sh` was run
   with a nonexistent fleet file: the CLI failed ONLY on the
   missing file, during parameter parsing, before any network
   call — proving all 10 flags parse. Same for `add-steps` (fails
   only on the missing steps file). A bogus flag is rejected with
   `Unknown options`, proving the parser is live.
2. **Wire-shape validation.** The RunJobFlow parameters the
   `create-cluster` command would produce (fleet.json, bootstrap
   action, tags, roles) and the AddJobFlowSteps parameters from
   `add_job_step.sh`'s step JSON (extracted verbatim from the
   script's heredoc) both pass botocore's `validate_parameters`
   against the EMR service model. The instance-groups fallback
   validates the same way.
3. **Shape-member audit.** Every key in `fleet.json`,
   `instance_groups.json` and the step JSON is a member of its
   service-model shape (`InstanceFleetConfig`,
   `InstanceTypeConfig`, `EbsConfiguration`,
   `VolumeSpecification`, `InstanceFleetProvisioningSpecifications`,
   `OnDemandProvisioningSpecification`, `InstanceGroupConfig`,
   `StepConfig`, `HadoopJarStepConfig`); fleet shape asserted
   (1 master fleet, 1 on-demand; 1 core fleet, 19 on-demand +
   0 spot).

Also verified by construction: the `create-cluster` command
contains no `--auto-terminate` (long-lived cluster, per the
workflow) and no create-time `--steps` (the job is triggered
separately); the `add-steps` JSON uses `command-runner.jar` with
the full spark-submit argument list, matching the runbook inline.

## V6 — Placement-gate logic: PASS (synthetic event logs)

**No real pod event log was reachable locally** (the canonical
record is on the RunPod research-ops volume at
`/workspace/blind-pod/`, which is not mounted in this sandbox;
the local `results/spark_cluster/` holds only the derived JSONs).
The gate was therefore tested against synthetic event logs written
in Spark's exact JsonProtocol format (the same field names
`pod_multihost/collect_results.py` parses), with per-pipeline
shuffle-read maxima equal to the Databricks 100M loads, plus a
matching synthetic manifest and the real 100M `counts.npz`:

| Case | Expected | Observed |
|---|---|---|
| Executors on 2 hosts | PASS, exit 0 | **PASS, exit 0** — all 6 gates green; verdicts 9/9; threshold audit 9/9 at 2M, 7/9 at 200k; A5 9/9 Q 1.085; EMR-vs-DBX loads ratio 1.0000 on all 9 |
| Executors on 1 host | VOID, exit 1 | **VOID, exit 1** — `placement` FAIL, "executors ran on ONE host only" |
| No host info anywhere | CANNOT DETERMINE, exit 1 | **CANNOT DETERMINE, exit 1** — `placement` FAIL, loud, never passes silently |

The gate is defensive by design: it reads hosts from BOTH
`SparkListenerExecutorAdded` and `SparkListenerTaskEnd` Task Info,
skips `.inprogress` files with a warning, and fails LOUD whenever
placement cannot be determined. The two-host case additionally
reproduces the full Databricks 100M comparison through the
verifier's own code path (not just the gate).

## Copied-artifact correction (one line)

`config/dbx_gt_100m.json` is a copy of the Databricks 100M ground
truth (`cloud_program/results/d-scale-100m/gt_databricks_zipf_s101_100m.json`).
The original carries a stale note, `"t_load_note": "T=200,000 per
the pre-registered design..."`, while its own `t_load` field is
`2000000.0` and all 9 verdicts are computed at 2,000,000. The
package copy corrects the note to T=2,000,000 and records the
correction inline; no numeric field was changed, and the verifier
reads `t_load`, never the note. The original file is untouched.

## S3 layout audit: PASS

Every S3 path referenced by the scripts, the job and the runbook
was grepped and checked against the upload layout
(`s3://BUCKET/PREFIX/{code,config,scripts,docs}/`):
`code/spark_job.py` and `code/bootstrap_action.sh` are the only
code paths referenced; the job writes `data/`, `output/` and
`eventlog/`; the verifier downloads `output/`, `eventlog/` and
`data/counts.*`; the cluster log URI is `logs/`. No reference
points outside the prefix.

## Cost estimate (for the runbook)

us-east-1 on-demand, prices checked 2026-11-23: EC2 m5.xlarge
$0.192/hr + EMR fee $0.048/hr = $0.240/hr/node; 20 nodes =
$4.80/hr; EBS gp3 64 GB x 20 ≈ $0.14/hr; S3 negligible. ~1 hour
all-in ≈ **$5** (2 hours ≈ $10). Basis: AWS-published rates
(EC2 on-demand $0.192/hr corroborated by the AWS price list of
Sep–Oct 2026 and multiple independent trackers; EMR fee $0.048/hr
for m5.xlarge in us-east-1). EMR bills per second with a
one-minute minimum.

## What was NOT validated (honest list)

1. **The Spark job has never executed.** This sandbox blocks ALL
   JVM-originated TCP, so no Spark job can run here. The job's
   Spark code is the pod's (executed on Spark 3.5.3) plus the
   Databricks D8 measurement (executed on Spark 4.2.0) — but
   their combination on EMR YARN, the S3A read/write paths, and
   the boto3 upload from the driver are proven only by the EMR
   run. If the step fails, the failure protocol in the runbook
   says what to send back.
2. **No live AWS call was made.** Cluster creation, the
   bootstrap action on real EMR nodes, provisioning time, and
   the EMR release's Python paths are unproven until the user
   runs step 3. The bootstrap is defensive (installs into every
   python3.x present; fails the cluster loudly if pip fails).
3. **No real event log was available** (see V6). The gate logic
   is tested against faithful synthetic logs, not a production
   EMR event log.
4. **EMR release choice.** `emr-7.14.0` (Spark 3.5.8-amzn-0,
   Python 3.9/3.11, standard support until Sep 2028) was chosen
   from the AWS release tables (checked 2026-11-23). If AWS has
   retired it by run time, any emr-7.x with Spark 3.5.x works —
   the job records the actual Spark version in the manifest.
   `emr-7.6.0` (Spark 3.5.3, the pod's exact version) is the
   closest alternative.
5. **The 100M peak driver RSS is measured: 2.92 GB**
   (single-process rerun; the 10M peak was 0.39 GB, so the
   marginal scaling is sub-linear — most of the 10M figure is
   fixed Python/numpy overhead). Driver memory is set to 8g,
   ~2.7x the measured peak.

## Appendix — dry-run log (100M rerun, single process)

```
SHA GATE PASS: counts_sha256=3549c275165e4e75c7de2e74c1b8d912cba4ee54141d7ff8db3abc36cd11dd8f
 "gen_seconds": 72.86
fact: rows=100000000 (want 100000000) row_groups=100 size=1497.8 MB
fact2: rows=60000000 (want 60000000) row_groups=60 size=234.9 MB
dim: rows=20000 row_groups=1 size=0.1 MB
DRY RUN 100m: PASS (SHA exact, parquet row counts exact, peak RSS 2.92 GB)
```

## Post-validation change (recorded, not hidden)

After the dry run, `flush_manifest` in `spark_job.py` was hardened:
intermediate flushes now retry 3x (10 s apart) and warn-and-continue
on persistent failure instead of killing the run; the final flush
is fatal (raises if the manifest cannot reach S3). The change is
confined to the Spark phase, which cannot execute in this sandbox
anyway; it was verified by `py_compile` plus a unit test that
extracts the actual function from the file by AST and runs it
against a mocked `s3_upload` (retry-then-succeed, always-fail
non-fatal, always-fail fatal — all three behave as specified).
No other package code changed after validation.
