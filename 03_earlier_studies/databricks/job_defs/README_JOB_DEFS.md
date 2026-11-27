# Job definitions — operator guide

Jobs API 2.2 JSON definitions, one per Databricks arm. All jobs run on
**serverless Jobs compute** (no cluster spec; tasks reference the
job-level `environments` entry with `environment_version: "2"` and pip
deps `numpy==2.5.4 pyarrow`). All jobs have `max_retries: 0` at job and
task level: a retry would double-bill and muddy provenance — a failed
task is re-run deliberately by the operator with a new run tag, never
auto-retried.

| File | Arm | Tasks | Timeout |
|---|---|---|---|
| `d_dbx.json` | D-DBX | gen (10M) → pipelines (9) | 7200 s |
| `d_scale_40m.json` | D-SCALE 40M | gen (40M/24M) → pipelines | 14400 s |
| `d_scale_100m.json` | D-SCALE 100M | gen (100M/60M) → pipelines + 1% sample export | 14400 s |
| `d_fb.json` | D-FB | gen (10M, own subdir) → feedback (7 pipelines + outeq spot check) | 7200 s |
| `d_neg.json` | D-NEG | negfam only (no gen task — data uploaded first) | 3600 s |

Names follow `bas-<arm>-RUN_TAG`; the operator substitutes RUN_TAG with
the run date (e.g. `20261125`) via `dbx_driver.py create-job --run-tag`.
Tags on every job: `project=blind-at-scale`, `arm=<arm>`,
`owner=<AUTHOR_EMAIL>` (spend attribution via
`system.billing.usage`).

## 0. Prerequisites (once, before any job)

**Gate G0 first (cost $0, read-only):**

```bash
# PAT from the author (Settings -> Developer -> Access tokens in the workspace).
# The PAT is read from stdin; it is never stored, logged, or put on a command line.
echo "<PAT>" | python3 ../dbx_driver.py --token-stdin probe
```

`probe` lists clusters, workspace status, and warehouses. Record the
outcome in the run log:

- **G0-serverless (expected):** `clusters/list` shows no classic
  clusters (or the API reports none creatable). Proceed with the job
  defs in this directory — they are the serverless path (D-DBX-S).
- **G0-classic (unexpected):** classic clusters ARE creatable. This is
  an **author decision**: D-DBX-C (Jobs cluster, 1 driver + 2 workers)
  is the stronger topology proof (named nodes, downloadable event log)
  and the cheaper run. **No classic job definition is built in this
  package** — the execution phase builds it from
  `spark_pipelines_dbx.py` if G0-classic fires (same script, same
  parameters, a `job_cluster_key` task spec instead of the serverless
  environment). Do not improvise the classic def; record the author's
  decision and build it deliberately.

Also confirm the P0 pre-checks in
`../CREDIT_AND_INTERFERENCE_REPORT.md` §7 (trial balance ≥ $50
Databricks, trial end date, D2-X status).

**Create the Unity Catalog objects** (once; via the workspace SQL
editor or a SQL warehouse — Blind at Scale's own catalog, NOT VISTA's
`verdictbench_d1`):

```sql
CREATE CATALOG IF NOT EXISTS blindatscale_dbx;
CREATE SCHEMA IF NOT EXISTS blindatscale_dbx.bas_dbx_smoke;
CREATE SCHEMA IF NOT EXISTS blindatscale_dbx.bas_dbx_run;
CREATE VOLUME IF NOT EXISTS blindatscale_dbx.bas_dbx_run.bas_vol;
```

Volume paths in the job defs are
`/Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/<arm-data-dir>/`.

## 1. Upload the scripts to the workspace

The job defs reference scripts at
`/Workspace/blind-at-scale/cloud_program/databricks/*.py`. Upload each
script with the driver (workspace-file target):

```bash
cd cloud_program/databricks
for f in gen_on_dbx.py spark_pipelines_dbx.py feedback_dbx.py negfam_dbx.py; do
  echo "<PAT>" | python3 dbx_driver.py --token-stdin upload-file \
      "$f" "/Workspace/blind-at-scale/cloud_program/databricks/$f" \
      --target workspace
done
```

(Verify each upload by listing the workspace folder in the console or
re-running `probe`'s workspace check. The driver logs every request to
`./dbx_runs/<timestamp>_upload-file/`.)

## 2. Create a job

```bash
# Dry-run first (prints the request, sends nothing, needs no token):
python3 dbx_driver.py --dry-run create-job job_defs/d_dbx.json --run-tag 20261125

# For real:
echo "<PAT>" | python3 dbx_driver.py --token-stdin create-job \
    job_defs/d_dbx.json --run-tag 20261125
# -> created job_id=<id>; record the job_id in the run log.
```

`--run-tag` is required: the defs contain the literal `RUN_TAG`
placeholder and `create-job` refuses to submit with it unfilled.

## 3. Run a job, fetch its output

```bash
echo "<PAT>" | python3 dbx_driver.py --token-stdin run-job <job_id>
# -> prints the run URL; polls to a terminal state (default timeout 4 h).

echo "<PAT>" | python3 dbx_driver.py --token-stdin get-output <run_id>
# -> run + per-task outputs saved under ./dbx_runs/<timestamp>_get-output/
```

After the run, download the results from the volume:

```bash
echo "<PAT>" | python3 dbx_driver.py --token-stdin download-results \
    /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/zipf_s101/run_manifest.json \
    ./results/d-dbx/run_manifest.json
echo "<PAT>" | python3 dbx_driver.py --token-stdin download-results \
    /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/zipf_s101/counts.npz \
    ./results/d-dbx/counts.npz
```

(D-SCALE 100M: also download `counts.npz` and the
`sample_1pct.parquet` directory for the local arms run. Note:
`download-results` fetches one file; a parquet directory needs one
fetch per part file, or the operator downloads via the workspace UI.
Record which route was used.)

## 4. Per-arm notes

- **D-DBX (`d_dbx.json`):** run first. After it completes, re-base the
  remaining Databricks estimates from its measured DBU consumption
  (`system.billing.usage`) before proceeding (PROGRAM_DESIGN.md §5).
  Success needs ≥ 2 distinct executor hosts in the manifest.
- **D-SCALE 40M (`d_scale_40m.json`):** predictions already exist
  (`data/zipf_s101_40m/predictions_main.csv`). No SHA anchor at 40M —
  verify via exact row counts + the local 40M max-count record + the
  exported counts.npz re-derivation.
- **D-SCALE 100M (`d_scale_100m.json`):** also exports `counts.npz`,
  the deterministic 1% sample (`hash(s) % 100 == 0`, ~1M rows), and
  the exact per-key `COUNT(DISTINCT s)` for kA
  (`distinct_A.parquet`) for the LOCAL arms run on the VM. Do not
  compare against predictions from a different scale. The distinct_A
  export is REQUIRED: `arms.run_arms` indexes `counts['distinct_A']`
  (D_A ground truth only) and would `KeyError` without it. Merge it
  into the downloaded `counts.npz` before the local arms run:

  ```python
  import numpy as np, pyarrow.parquet as pq
  z = dict(np.load('counts_100m.npz'))
  t = pq.read_table('distinct_A.parquet')
  arr = np.zeros(20000, dtype=np.int64)
  arr[t.column('kA').to_numpy()] = t.column('d').to_numpy()
  z['distinct_A'] = arr
  np.savez_compressed('counts_100m.npz', **z)
  ```

  Note: the Databricks-derived distinct_A uses the same definition as
  the local one (`COUNT(DISTINCT s)` per kA) but the (kA, s) pairing
  differs from the local generator's (row order is not reproduced
  operation-for-operation — see `gen_on_dbx.py`). The difference is
  confined to D_A, which is not a cloud-compared pipeline, and cannot
  flip D_A's verdict (max distinct ~100,000 vs T_DISTINCT 50,000).
  Driver-memory risk: the 100M numpy generation holds ~100M-row
  int32 columns driver-side; if the gen task OOMs, that is a recorded
  finding.
- **D-FB (`d_fb.json`):** generates its own 10M dataset in
  `fb_zipf_s101/` (no dataset shared between arms). Includes the
  salt-determinism check and the `--sample-outeq` spot check. If the
  cap binds, the outeq spot check is the first trim (remove the flag
  from the def — a deliberate edit, recorded in the run log).
- **D-NEG (`d_neg.json`):** NO gen task. Upload the negfam data FIRST:

  ```bash
  # From ~/workspace/blind-at-scale/data/negfam_s101/ (89 MB total):
  for f in fact.parquet dim.parquet predictions_negfam.csv gt_negfam.json; do
    echo "<PAT>" | python3 dbx_driver.py --token-stdin upload-file \
        ~/workspace/blind-at-scale/data/negfam_s101/$f \
        /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/negfam_s101/$f \
        --target volume
  done
  ```

  The job's `negfam_dbx.py` verifies the upload (row counts, total
  payload bytes, max per-key row counts vs `gt_negfam.json`) and
  records the result in its manifest.

## 5. What was NOT validated locally (open questions)

These can only be answered by execution (PROGRAM_DESIGN.md §9):

1. Whether `environment_version: "2"` maps to a Python 3.12 runtime
   in this workspace, and whether the pip pin `numpy==2.5.4` installs
   cleanly in the serverless environment. The SHA gate in
   `gen_on_dbx.py` is the arbiter: if the runtime numpy cannot
   reproduce the draws, the arm fails loudly at gen time (and the
   fallback is uploading the local parquet — PROGRAM_DESIGN.md §6).
2. Whether `spark.databricks.photon.enabled=false` is honoured on
   serverless (D9 fires if Photon is forced on — recorded, not
   hidden).
3. Whether `sc.statusTracker()` exposes distinct executor hostnames
   on serverless (decides whether D-DBX-S can prove topology).
4. Whether the Spark event log is downloadable from a serverless job
   run (decides whether the D8 cross-check is available).
5. Actual serverless-jobs DBU consumption for a 10M 9-pipeline run
   (re-bases all Databricks estimates after D-DBX).
6. The 100M gen task's driver memory headroom on serverless.

The JSON defs were validated locally only for structure (json.load +
required-keys + no unfilled placeholder except RUN_TAG). No Jobs API
call was made during preparation.
