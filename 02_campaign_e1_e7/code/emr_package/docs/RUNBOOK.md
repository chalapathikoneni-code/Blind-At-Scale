# Blind at Scale — E1 + E6 EMR Runbook (FINAL, one package)

All experiments, **one cluster, one step sequence**. **No bootstrap
action, nothing installs at cluster start** — the jobs use only
pyspark + the Python standard library. That failure mode is gone.

- **E1** — real join validation on OpenAlex (natural data, natural
  skew): prediction arms vs the actually executed join, at three
  scales, plus a second natural join family (works × sources).
- **E6** — the same joins with AQE off vs on.
- **Steps 5 and 7 are the SYNTHETIC extras** — see the note below.

Cluster: 1 master + 19 core, m5.xlarge, emr-7.14.0 (Spark 3.5.8) —
the same shape as the two successful runs. Everything lands under
`s3://YOUR_BUCKET/Access/e1e6/`. Cost ~$5/hr while it lives; **section 6
stops the billing.**

> **The core is the OpenAlex real-data steps (0–4 and 6).**
> Steps 5 and 7 reuse the synthetic parquet already in S3 from the
> Nov 23 run (`s3://YOUR_BUCKET/Access/synthetic/data/`). You do **not**
> need to run both datasets: Steps 5 and 7 can simply be left out of
> the step list (add the steps individually instead of `all`) and
> nothing else is affected.
> - **Step 5 (A9 at 100M) is recommended** — it closes the
>   A9-at-100M hole: the classical end-biased estimator run against
>   the 100M synthetic corpus, directly comparable to the
>   local-scale Experiment A outputs. It needs no new data.
> - **Step 7 (E6 on synthetic J_A/J_N) is optional** — AQE off vs on
>   for the known A1-miss skewed joins.
> If `Access/synthetic/data/fact.parquet` is missing, Steps 5/7 fail
> loud — tell us, and just skip them.

## 1. Upload the package to S3

```bash
unzip emr_package_final.zip && cd emr_package_final
scripts/upload_to_s3.sh        # ends with UPLOAD OK
```

## 2. Create the cluster

```bash
scripts/emr_create_cluster.sh  # prints the cluster id j-...
```

Wait until WAITING (~10 min):

```bash
aws emr describe-cluster --cluster-id j-XXXX --region us-east-1 \
  --query 'Cluster.Status.State' --output text
```

## 3. Add the steps — one at a time, in order

**Run Step 0 (smoke) first and read its result before adding
anything else.** It takes ~2 minutes and proves the whole path —
S3 read/write, spark-submit, event log. If smoke fails, stop and
send the step log rather than pushing on: every later step depends
on the same path smoke proves.

Each step is independent (ActionOnFailure=CONTINUE); a failed step
does not block the next. Add the next one only after the previous
shows COMPLETED — or run `scripts/add_steps.sh all` to queue the
whole sequence at once; EMR runs steps sequentially in submission
order.

```bash
export CLUSTER_ID=j-XXXX
scripts/add_steps.sh smoke     # Step 0: ~2 min. Proves the path.
scripts/add_steps.sh prep      # Step 1: OpenAlex download + tables
scripts/add_steps.sh e1        # Step 2: E1 join validation
scripts/add_steps.sh e6        # Steps 3+4: AQE off, then AQE on
scripts/add_steps.sh a9        # Step 5: A9 at 100M (synthetic, recommended)
scripts/add_steps.sh e1more    # Step 6: E1 at 8 & 12 parts + works×sources
scripts/add_steps.sh e6synth   # Steps 7a+7b: AQE off/on, synthetic (optional)
```

Watch them:

```bash
aws emr list-steps --cluster-id $CLUSTER_ID --region us-east-1 \
  --query 'Steps[*].{Name:Name,State:Status.State}' --output table
```

What each step does:

| Step | Job | Done looks like |
|---|---|---|
| 0 smoke | tiny join on generated data | `output/smoke.json`, status OK |
| 1 prep | downloads 4 OpenAlex works parts (~4.2 GB, ~1.54M works) from the public snapshot into S3, builds fact/dim tables, prints the top-30 author frequencies (the skew) in the step log | `output/prep_summary.json` |
| 2 E1 | 3 join cases × 3 prediction arms, then the full join executes; one JSON per case | `output/e1_*.json` |
| 3 E6 off | the 2 skewed cases, AQE off | `output/e6_off_*.json` |
| 4 E6 on | same, AQE on + skew-join on; plan text recorded | `output/e6_on_*.json` |
| 5 A9 at 100M *(synthetic, recommended)* | the classical end-biased estimator (the paper's A9) against the 100M synthetic corpus parquet already in S3. Spark computes the per-key counts; writes A9's predicted loads, verdicts at T = 2,000,000, and the packet byte size | `output/a9_100m.json` + `output/a9_100m_csv.tmp/` |
| 6 E1 scales + sources | stages OpenAlex parts up to 12 (~12.6 GB raw) and reruns the authorship join cases at 8 and 12 parts (Step 2 was 4 parts) — scale by MORE REAL DATA, never duplicated rows. Plus the second natural family: works × sources (venues) join + venue self-join | `output/e1_*_s8.json`, `e1_*_s12.json`, `e1_join_source_dim.json`, `e1_selfjoin_source.json` |
| 7a/7b E6 synthetic *(optional)* | AQE off vs on for the synthetic J_A / J_N skewed joins (the known A1-miss cases), on the Nov 23 synthetic parquet | `output/e6_off_synth_*.json`, `output/e6_on_synth_*.json` |

If a step FAILS: open its log in the EMR console (Steps → step →
View logs) and send back the tail. Do not re-run a failed step
until the cause is read.

## 4. While you are in S3 — the retrieval checklist (2 minutes)

Copy these existing files into a new folder
`s3://YOUR_BUCKET/Access/e1e6/return/` (console: select → Copy, or CLI
below). They are the as-run code from the November runs, needed to
close the reproducibility item:

```bash
for f in bootstrap_action.sh spark_job.py; do
  aws s3 cp s3://YOUR_BUCKET/Access/synthetic/code/$f \
    s3://YOUR_BUCKET/Access/e1e6/return/synthetic_$f; done
for f in bootstrap_action.sh spark_job_realdata.py realdata_model.py stage_data.py; do
  aws s3 cp s3://YOUR_BUCKET/Access/realdata/code/$f \
    s3://YOUR_BUCKET/Access/e1e6/return/realdata_$f; done
for f in counts.npz counts_summary.json meta.json; do
  aws s3 cp s3://YOUR_BUCKET/Access/synthetic/data/$f \
    s3://YOUR_BUCKET/Access/e1e6/return/$f || true; done
```

Any file that 404s: note which one, move on.

## 5. Download back

```bash
aws s3 sync s3://YOUR_BUCKET/Access/e1e6/output/   ./e1e6_output/
aws s3 sync s3://YOUR_BUCKET/Access/e1e6/eventlog/ ./e1e6_eventlog/
aws s3 sync s3://YOUR_BUCKET/Access/e1e6/logs/     ./e1e6_logs/
aws s3 sync s3://YOUR_BUCKET/Access/e1e6/return/   ./e1e6_return/
```

Send back all four folders (zip them if easier). The event logs are
the physical evidence — do not skip them.

## 6. Terminate the cluster

```bash
aws emr terminate-clusters --cluster-ids $CLUSTER_ID --region us-east-1
```

## Notes

- Thresholds (frozen, the study convention): a case FAILs if the
  max partition load exceeds 2× the mean load (relative) or
  0.02 × total rows (absolute); both are reported.
- E1 safety: a case whose predicted output exceeds 2 billion rows
  is skipped with the reason recorded — nothing can exhaust the
  cluster.
- Predictions assign keys to partitions with crc32 (documented
  approximation). The event log is the authoritative measurement;
  it gets verified after you send it back.
