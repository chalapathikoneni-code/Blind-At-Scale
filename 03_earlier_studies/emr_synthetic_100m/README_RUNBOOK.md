# Blind at Scale — EMR 100M Multi-Node Run: Runbook

**What this is.** One run of the 9 headline pipelines (G_A, G_B, G_N,
J_A, J_B, J_N, F_A, F_B, W_A) on the 100M-row zipf dataset, on a real
20-node Spark cluster on AWS EMR. It is the run that retires the last
structural objection to the paper (hostile review R2): the title says
"Distributed", and this run proves the ground truth was measured on
genuinely multi-node execution — executors on >= 2 distinct hosts,
proven from the Spark event log.

**What it is not.** There is no 10M run in this package. The 100M run
is the run. The Databricks 100M leg (Nov 22–23) already measured the
same dataset on serverless Spark; this run reproduces it on a real
cluster, and the verifier compares the two.

**Your workflow, in order.** The steps below follow exactly how you
said you will run it: download from Drive → put it in S3 → spin up
the cluster → trigger the EMR step → verify → terminate. Each step
says what to type and what "done" looks like.

**Time.** About 15 minutes of your time plus ~45–60 minutes of
cluster time (mostly provisioning). The job itself runs ~10–20
minutes once the cluster is ready.

**Cost.** ~$5 for a one-hour run (see the cost section below).
Forgetting to terminate the cluster is the only way the bill grows.

---

## 0. Before you start — prereqs and preflight

**You need:**

- AWS CLI v2, configured against your office account
  (`aws configure` done; the profile you use must be able to create
  EMR clusters, read/write S3, and use the default EMR roles).
- An S3 bucket you own, in the region you will run in. The run
  writes only under `s3://<bucket>/blind-at-scale/emr-100m/`
  (code, data, output, event log, cluster logs). Nothing else in
  the bucket is touched.
- Python 3 on your local machine (for the verifier in step 6;
  numpy is optional — the verifier uses it if present).
- The default EMR service roles. If this account has never run
  EMR, create them once (harmless if they already exist):
  `aws emr create-default-roles --region us-east-1`

**Preflight — run these before anything else.** All three must
succeed; if any fails, fix it before step 1.

```bash
aws sts get-caller-identity
aws s3 ls s3://YOUR-BUCKET-HERE/
aws emr list-clusters --active --region us-east-1
```

**Region.** The scripts default to `us-east-1`. If your bucket or
your office account's EMR setup lives elsewhere, change `REGION`
at the top of all three scripts to the same value — the bucket,
the cluster and the job must all be in one region.

## 1. Download the package from Drive

Download `emr_package.zip` from the Drive link and unzip it:

```bash
unzip emr_package.zip
cd emr_package
```

**What is inside.** No nested zips, no deep nesting:

```
emr_package/
  code/
    spark_job.py           the job: generator + 9 pipelines (the dataset IS this file)
    bootstrap_action.sh    installs pinned numpy 1.23.5 + pyarrow 14.0.2 on every node
  config/
    fleet.json             instance fleet: 1 master + 19 core, m5.xlarge
    instance_groups.json   fallback if fleets do not suit your account
    predictions_main.csv   local 100M arm predictions (66 rows, 6 arms)
    dbx_gt_100m.json       Databricks 100M ground truth (comparison target)
    summary_100m.json      Databricks 100M headline figures
  scripts/
    upload_to_s3.sh        step 2
    emr_create_cluster.sh  step 3
    add_job_step.sh        step 4
    verify_emr_run.py      step 6
  docs/
    README_RUNBOOK.md      this file
    VALIDATION_REPORT.md   what was validated locally, and how
```

**About the data — please read this.** There is no multi-GB data
file in the zip, and that is deliberate. The 100M dataset is fully
determined by seeds: the generator inside `spark_job.py` reproduces
the exact same 100M rows every time it runs (the same generator the
pod and Databricks runs used), and a SHA-256 hash over the per-key
counts proves it. The dataset is the generator + the seeds + the
hash — same bytes every time. A ~2 GB file upload would only add
failure modes (a truncated or corrupted upload would silently
produce wrong ground truth). On the cluster, generation takes about
a minute; the SHA gate aborts the job loudly if the counts ever
differ from the Databricks 100M anchor. All data is synthetic from
seeds; nothing proprietary is anywhere in this package.

## 2. Upload the package to S3

Edit the `BUCKET` line at the top of `scripts/upload_to_s3.sh`
(keep `PREFIX=blind-at-scale/emr-100m` unless you have a reason to
change it — and use the SAME bucket and prefix in all three
scripts), then:

```bash
scripts/upload_to_s3.sh
```

Done looks like: the script lists the uploaded keys back and ends
with `UPLOAD OK`. It fails loudly if `code/spark_job.py` or
`code/bootstrap_action.sh` did not land.

## 3. Spin up the cluster

Edit `BUCKET` at the top of `scripts/emr_create_cluster.sh`
(same values as step 2), then:

```bash
scripts/emr_create_cluster.sh
```

This creates a **long-lived** cluster — 1 master + 19 core nodes,
m5.xlarge, Spark 3.5.8 (emr-7.14.0) — with the bootstrap action
from step 2. It does NOT start the job: you trigger that separately
in step 4. There is NO auto-terminate; the cluster stays up until
you terminate it in step 7.

Done looks like: the script prints a cluster id (`j-...`).
**Save it** — you need it for steps 4–7.

Wait for the cluster to reach WAITING (provisioning + bootstrap
takes ~10–15 minutes):

```bash
aws emr describe-cluster --cluster-id j-XXXX --region us-east-1 \
  --query 'Cluster.Status.State' --output text
```

If the cluster goes to `TERMINATED_WITH_ERRORS` instead, the
bootstrap action failed — see "If the cluster fails to start" in
the failure section below. Do not proceed to step 4 until the
state is `WAITING`.

**Fleet knobs (config/fleet.json).** The default is 19 core nodes,
all on-demand, m5.xlarge with m6i.xlarge as an alternate type.
To use Spot for part of the core fleet, change the CoreFleet
entry: lower `TargetOnDemandCapacity` and set `TargetSpotCapacity`
to the remainder (e.g. 5 on-demand + 14 Spot). Spot saves ~70% of
the EC2 line on those nodes; the EMR fee stays at the on-demand
rate, and Spot nodes can be reclaimed mid-run (for a ~15-minute
job on a 1-hour cluster this is a small risk, but on-demand is the
safe default and it is already only ~$5). To change the fleet
size, change `TargetOnDemandCapacity` / `TargetSpotCapacity`
(their sum is the core count). If your account does not support
instance fleets, use `config/instance_groups.json` instead — the
fallback command is in the appendix.

## 4. Trigger the job as an EMR step

```bash
CLUSTER_ID=j-XXXX scripts/add_job_step.sh
```

(or `scripts/add_job_step.sh j-XXXX`). This adds ONE step that
runs:

```
spark-submit --master yarn --deploy-mode cluster \
  --driver-memory 8g --executor-memory 8g --executor-cores 4 \
  --num-executors 18 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.sql.shuffle.partitions=128 \
  --conf spark.sql.files.maxPartitionBytes=8388608 \
  --conf spark.sql.autoBroadcastJoinThreshold=-1 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=s3a://BUCKET/PREFIX/eventlog/ \
  s3://BUCKET/PREFIX/code/spark_job.py \
  --s3-bucket BUCKET --s3-prefix PREFIX
```

Why these values: 18 executors x 4 cores = 72 slots across the 19
core nodes (one node hosts the driver in cluster mode); 8g driver
memory holds the 100M generation (~3 GB peak, measured locally);
dynamic allocation is OFF so the executor set stays fixed for the
topology proof; `maxAppAttempts=1` so a failure fails LOUD once
instead of re-running and confusing the event log.

Done looks like: the script prints a step id (`s-...`).

**What the job does, in order:** (1) checks numpy is exactly
1.23.5 — the 100M anchor was generated under 1.23.5, and numpy
2.5.4 provably produces different draws at 100M, so any other
version aborts the job before anything runs; (2) generates the
100M dataset on the driver (~1 minute) and checks the counts
SHA-256 against the Databricks anchor — a mismatch aborts loudly;
(3) uploads the parquet + counts to `s3://BUCKET/PREFIX/data/`;
(4) runs the 9 pipelines on YARN, writing the event log to
`s3://BUCKET/PREFIX/eventlog/` and the manifest to
`s3://BUCKET/PREFIX/output/run_manifest.json` after every
pipeline. The F_A/F_B joins are NEVER materialised (F_A's exact
output is ~5.5e13 rows) — their loads are computed analytically
from Spark-produced counts, exactly as in the pod and Databricks
runs.

## 5. Monitor

```bash
aws emr describe-step --cluster-id j-XXXX --step-id s-YYYY \
  --region us-east-1 --query 'Step.Status.State' --output text
```

States: `PENDING` → `RUNNING` → `COMPLETED` (or `FAILED`).
The job takes ~10–20 minutes once RUNNING. You can also watch it
in the EMR console (cluster → Steps), and the Spark UI is
available from the console while the step runs.

To see the job's own progress (SHA gate result, per-pipeline
loads), check the step's stdout in the console (Steps → step →
"View logs"), or after completion:

```bash
aws s3 cp s3://BUCKET/PREFIX/output/job_summary.json - --region us-east-1
```

That file carries the counts gate result, the provisional
in-job verdicts, and the executor topology the job saw. Note:
those verdicts are provisional — the event-log verdicts from
step 6 decide.

## 6. Verify the run (locally)

Do not skip this step. It is the part that turns "a Spark job
ran" into "multi-node ground truth":

```bash
python3 scripts/verify_emr_run.py --bucket YOUR-BUCKET-HERE \
  --prefix blind-at-scale/emr-100m
```

It downloads the manifest, event log and counts from S3 and runs
six gates:

| Gate | What it proves |
|---|---|
| `counts_sha_manifest` / `counts_sha_npz` | the dataset on the cluster is the anchor dataset (SHA 3549c275…) |
| `placement` | executors ran on **>= 2 distinct hosts** (the multi-node proof) |
| `all_9_pipelines` | all 9 pipelines have event-log ground truth |
| `threshold_audit` | the 9 verdicts recompute 9/9 at T = 2,000,000 (and 7/9 at T/10, the Databricks pattern) |
| `d8_cross_check` | the in-job partition-id loads agree with the event-log loads (same shuffle, two measurements) |

Then it prints the comparison table: EMR loads vs Databricks
100M loads (same dataset, same partitioner — expect ratios near
1.0000), local-simulation agreement, and every arm's verdict
agreement vs EMR.

**Exit code 0 + `VERIFICATION PASSED`** means the run retires
the R2 objection. The file to keep is
`emr_verify_out/emr_ground_truth_100m.json` — that is the
ground-truth file the paper cites, plus
`emr_verify_out/verification_report.json` (every gate,
machine-readable). Send both back and the paper gets updated
from them.

**Exit code 1 + `VERIFICATION FAILED`** means do NOT cite this
run. The failing gate is named in the output. See the failure
section below.

## 7. Terminate the cluster

```bash
aws emr terminate-clusters --cluster-ids j-XXXX --region us-east-1
```

Then confirm it is gone:

```bash
aws emr describe-cluster --cluster-id j-XXXX --region us-east-1 \
  --query 'Cluster.Status.State' --output text
# expect TERMINATED
```

**This is the step that stops the billing.** The cluster has no
auto-terminate; every hour you leave it running costs ~$4.94.
Terminate it as soon as step 6 is done (the results are all in
S3 — terminating the cluster deletes nothing you need).

---

## Cost — full basis (prices checked 2026-11-23, us-east-1)

| Line | Rate | For this run |
|---|---|---|
| EC2 m5.xlarge on-demand | $0.192/hr/node | 20 nodes |
| EMR fee m5.xlarge | $0.048/hr/node | 20 nodes |
| Combined compute | $0.240/hr/node | **$4.80/hr** |
| EBS gp3 64 GB x 20 | $0.08/GB-month | ~$0.14/hr |
| S3 (a few GB, one day) | $0.023/GB-month | < $0.01 |
| **Total, ~1 hour all-in** | | **~$5** |

Basis: EC2 on-demand $0.192/hr and EMR fee $0.048/hr for
m5.xlarge in us-east-1, from the AWS price list (multiple
corroborating sources checked 2026-11-23, incl. the AWS-published
rates of Sep–Oct 2026); EMR bills per second with a one-minute
minimum, so you pay for the time the cluster actually exists.
The ~1-hour figure assumes ~15 min provisioning + ~15 min job +
slack; a 2-hour run is ~$10. If your office account has Savings
Plans or Reserved Instances, the EC2 line is lower; Spot core
nodes cut the EC2 line by ~70% on those nodes (the EMR fee is
unchanged). These are estimates from public list prices — your
account's actual rates govern.

## What was validated locally vs what only the EMR run can prove

Validated here, before you got the package (evidence in
docs/VALIDATION_REPORT.md):

- **The generator reproduces the anchor exactly.** Full 100M
  generation run locally with the package's own code under the
  pinned numpy 1.23.5: counts SHA-256 exactly
  `3549c275…dd8f`, all 7 maxima exact, parquet row counts exact.
  The same code path at 10M reproduces the 10M anchor
  (`f9f3fa8b…`) under numpy 2.5.4.
- **The numpy pin is load-bearing, and the wrong one was caught
  here, not on your cluster.** numpy 2.5.4 produces DIFFERENT
  100M draws (SHA `fd25cafc…`); the package pins 1.23.5 and the
  job aborts on any other version.
- **The verify/collect logic reproduces the Databricks 100M
  comparison exactly** (threshold audit 9/9 at 2M, 7/9 at 200k;
  A5 9/9, median Q 1.085; local sim 9/9, Q 1.121).
- **The placement gate was exercised end to end** against
  synthetic event logs in Spark's real format: 2 hosts → PASS,
  1 host → VOID, no host info → CANNOT DETERMINE (fails loud,
  never passes silently).
- **Every Spark API in the job exists in Spark 3.5** (audit
  table in the validation report); no Databricks-only API is
  used. Both Python files compile; all shell scripts pass
  shellcheck; the `create-cluster` / `add-steps` commands were
  validated flag-by-flag against the AWS CLI shapes (no live
  AWS call was made from here).

NOT validated here — only the EMR run can prove these:

- **That the Spark job executes correctly on YARN.** This
  sandbox blocks JVM network traffic, so no Spark job can run
  here at all. The job's Spark code is the pod's (executed on
  Spark 3.5.3) plus the Databricks D8 measurement (executed on
  Spark 4.2.0), both proven — but their combination on EMR
  YARN is proven only by your run.
- **That executors land on >= 2 hosts.** That is what the
  placement gate checks after the run. If it fails, the run is
  void as multi-node evidence — that outcome is a finding, not
  a bug in the package.
- **Exact EMR behaviour**: bootstrap timing, provisioning
  duration, the EMR release's Python paths. The bootstrap is
  written defensively (installs into every python3.x present)
  and fails the cluster loudly if it cannot install.

The safety nets are designed so that every failure mode above
fails LOUD (aborted job, failed gate, failed cluster creation)
rather than producing a wrong number that looks right.

## Failure protocol — what to do, and what to send back

**If the cluster fails to start (`TERMINATED_WITH_ERRORS`).**
Most likely the bootstrap action failed (nodes without internet
access cannot pip-install numpy). Check the bootstrap log in the
EMR console (cluster → Bootstrap actions) or at
`s3://BUCKET/PREFIX/logs/j-XXXX/bootstrap-actions/`. If it is a
network/pip failure, the fix is on the AWS side (a VPC endpoint
or NAT for PyPI, or pre-baked AMIs) — the package cannot work
around nodes that cannot reach PyPI, and the SHA gate would catch
a wrong-numpy run anyway. Send back: the bootstrap log tail.

**If the step FAILS.** Check the step log first (EMR console →
Steps → step → "View logs", or
`s3://BUCKET/PREFIX/logs/j-XXXX/steps/s-YYYY/`). The job prints
exactly which gate failed:
- `NUMPY GATE FAILED` — the bootstrap installed the wrong numpy
  (or none). Send back the bootstrap log.
- `SHA GATE FAILED` — the generated counts do not match the
  anchor. Send back the step log (it prints the mismatching
  values) — do not re-run until the cause is understood.
- `ROW COUNT MISMATCH` — Spark read a different number of rows
  than were generated. Send back the step log.
- Any other Spark error — send back the step log tail plus
  `job_summary.json` if it exists.

**If verification FAILS.** The output names the failing gate:
- `placement` VOID (1 host) — the cluster ran the job on one
  node. This can happen if YARN placed all executors on one
  host (unlikely with 19 core nodes) or if the job ran in local
  mode (check the manifest's `master` field — it must be
  `yarn`). Send back `verification_report.json` and the
  manifest.
- `placement` CANNOT DETERMINE — the event log has no host
  info. Check the event log downloaded (is it empty? only
  `.inprogress` files?). If the step is still RUNNING, wait for
  COMPLETED and re-run the verifier.
- `threshold_audit` or `d8_cross_check` — send back
  `verification_report.json`; these are findings to investigate,
  not automatic voids.
- Any gate — send back `emr_verify_out/` (both JSON files) and
  the step log tail.

**Re-running.** If a run fails for an infrastructure reason
(not a gate), re-running is cheap and safe: the job regenerates
the identical dataset (same SHA) and overwrites the S3 output.
Just run step 4 again on the same cluster (or a fresh cluster).
If a GATE failed (numpy, SHA, placement), do not re-run blindly
— send the logs back first.

## Appendix — instance-groups fallback

If your account does not support instance fleets, create the
cluster with instance groups instead (same 1 + 19 shape, same
bootstrap). Replace the `--instance-fleets` line in
`emr_create_cluster.sh` with:

```bash
--instance-groups file://config/instance_groups.json
```

(everything else in the command is unchanged).

## Appendix — the exact labelling for the paper

Once step 6 passes, the run is cited as: "AWS EMR multi-node
Spark cluster (YARN, emr-7.14.0 / Spark 3.5.8, 1 master + 19
core m5.xlarge); executor placement on N distinct hosts proven
from the Spark event log." The verifier prints the exact host
count — use that number, not "20 nodes" (nodes provisioned is
not the same claim as executors placed). If step 6 fails, the
run is not cited at all.
