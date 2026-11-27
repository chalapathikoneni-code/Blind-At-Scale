# Blind at Scale: Reproducibility Package

Code, count-level results, and verification records for the paper:

**Chalapathi Koneni, "Blind at Scale: Information Sufficiency for
Pre-Execution Resource-Risk Prediction in Distributed Data Pipelines"**
(submitted to IEEE Access).

The paper asks what a resource-risk predictor can know before a
distributed pipeline runs, and what it provably cannot. This package
contains the scripts that produced the paper's results, the
count-level outputs of those runs, and the independent verification
reports that tie every headline number in the paper to a raw
artifact. It contains no datasets: the real-data studies use public
data obtained from the original providers (see "Data" below).

## Directory map

| Directory | Contents |
|---|---|
| `01_selfjoin_verified/` | The OpenAlex self-join verification runs (Nov 26, 2026). `real_join/`: the as-run materialized self-join script and its result JSONs. `streaming_counter/`: the patched streaming per-partition counter scripts and their measured JSONs. |
| `02_campaign_e1_e7/` | The November 25-26 EMR campaign (experiments E1-E7). `code/emr_package/`: the packaged job code, cluster scripts, and runbook as uploaded for the campaign. `code/drivers/`: the E2-E7 driver and analysis scripts. `results/`: per-run result files from the campaign and the final captures. `data_ext_metadata/`: per-dataset metadata for the generated near-threshold corpora (binaries are regenerated, see below). `docs/`: the E1-E7 experiment reports and the frozen execution protocol. |
| `03_earlier_studies/` | The studies that precede the campaign. `local_study/`: the November 6 local study (99 instances) and Experiment A, code and results. `pod/`, `pod_multihost/`: the single-host Spark ground-truth scripts. `databricks/`: the Databricks validation scripts and job definitions. `snowflake/`: the Snowflake scripts (context only, see scope notes). `emr_synthetic_100m/`: verification and validation reports for the synthetic 100M EMR run. `nyc_realdata/`: the NYC TLC real-data arm scripts, corpus manifest, and verification report. |
| `04_verification/` | The audit record: the real-join and measured self-join verification reports, the final-capture verification, the final scientific verdict, the claim-to-evidence matrix, the updated number provenance, the reproducibility closure report, and the evidence audit with its gap register. |

`MANIFEST.sha256` hashes every file in the package.
`SANITIZATION.md` records exactly how this deposit differs from the
as-run originals (identifiers redacted, nothing else).

## Environment

EMR studies (campaign, self-join verification, synthetic 100M):

- Amazon EMR 7.14.0, Apache Spark 3.5.8-amzn-0.
- Frozen configuration for the verified runs, confirmed from the
  Spark event logs: `spark.sql.adaptive.enabled=false` (AQE off),
  `spark.sql.shuffle.partitions=128`, 18 executors on 18 hosts,
  executor memory 8 GB / 4 cores per executor, driver memory 8 GB,
  dynamic allocation off, `spark.yarn.maxAppAttempts=1`,
  `spark.sql.autoBroadcastJoinThreshold=-1`.
- Job code is submitted with `spark-submit`; the step definitions
  used for the real-join runs are preserved in
  `01_selfjoin_verified/real_join/step_commands/`.

Local study: Python 3 with DuckDB 2.0.0, pandas, NumPy, and SciPy
(see `03_earlier_studies/local_study/environment_requirements.txt`
for the exact pins used by the analysis environment).

## How to run each piece

- **Self-join, materialized (primary measurement).**
  `01_selfjoin_verified/real_join/code/e1_selfjoin_real.py` with
  `common.py` on the Spark path. Point `--s3-prefix` at a bucket
  holding the staged OpenAlex input (see Data) and `--out-prefix`
  at the output location. The script records its prediction to a
  `result_pre` JSON before the join executes, then writes the joined
  rows to Parquet and reads the count back. The runs in this package
  used `--force-measure` to override the output guard; both scales'
  inputs, predictions, and outputs are in `real_join/results/`.
- **Self-join, streaming counter.**
  `01_selfjoin_verified/streaming_counter/code/e1_join.py --cases
  selfjoin --force-measure` with the same `common.py`. This is the
  patched counter whose `count_per_partition` streams partition
  counts instead of materializing them. Its prediction functions
  (`predict_from_counts`, `verdict` in `common.py`) are
  byte-identical to the campaign package's; the verification report
  in `04_verification/` documents the diff.
- **Campaign E1-E7.** Follow
  `02_campaign_e1_e7/code/emr_package/docs/RUNBOOK.md`: upload the
  job code, create the cluster from `config/instance_groups.json`,
  add the steps. The E2-E7 drivers in `code/drivers/` run against
  the staged inputs and write the result files whose delivered
  copies are under `results/`.
- **Local study and Experiment A.**
  `03_earlier_studies/local_study/code/run_all.sh` regenerates the
  seeded corpora, computes exact ground truth, and scores every arm;
  `run_exp_a.py` and `score_exp_a.py` do the same for Experiment A.
  Delivered outputs are under `local_study/results/`.
- **Near-threshold corpora (E3).** `code/drivers/gen_ext.py`
  regenerates the generated datasets whose metadata is in
  `02_campaign_e1_e7/data_ext_metadata/`; the generated Parquet and
  DuckDB binaries are not redistributed (they are large and exactly
  reproducible from the generator and the recorded seeds).

## Data

No data files are included. The real-data studies use:

- **OpenAlex** (self-join and campaign inputs): a public snapshot,
  staged into per-part files as described in the runbook and the
  preparation script `prep_openalex.py`.
- **NYC TLC** trip records (real-data arm): public monthly files
  from the NYC Taxi and Limousine Commission; the exact file list
  and checksums are in `03_earlier_studies/nyc_realdata/
  corpus_manifest.json`.

Obtain both from the original providers. Synthetic corpora are
generated by the packaged generators from recorded seeds.

## Scope notes (read before citing a reproduction)

These notes use the manuscript's own wording where it states them.

- The study retains protocols, scripts, count-level results and
  engine logs in its evidence archive, including the E1-E7 campaign,
  the measured self-join recovery and the materialization follow-up.
  **The November EMR bootstrap and exact as-run code were not
  recovered in full; the archived job bundles cannot be represented
  as byte-identical to those executed.** The November synthetic and
  real-data EMR legs therefore reproduce at the level of their
  packaged code, manifests, and verification reports, not at the
  level of byte-identical as-run scripts. (The November 26 self-join
  scripts in `01_selfjoin_verified/` ARE the as-run scripts,
  received with the captures and hash-verified; see
  `04_verification/`.)
- The pod Spark ground truth was executed on a **single host** and
  is labelled as such wherever it is used; it carries no multi-node
  claim. The Databricks validation leg ran with one executor and
  likewise supports no multi-node claim.
- The Snowflake material is **weak evidence at the 10M scale only**;
  it is included as context for the paper's engine-check narrative
  and is not a reproduction target.
- Raw Spark event logs and step logs are retained in the author's
  evidence archive and are not part of this deposit (size); every
  number derived from them was recomputed in the verification
  reports in `04_verification/`, which record the log hashes.
- The gap register in `04_verification/GAP_REGISTER.csv` is the
  audit snapshot from before the November 26 returns; gaps G03 and
  G05 were closed by those returns, as the verification reports
  record. Gap G02 (the November as-run code above) and G04 (event
  logs for three failed campaign applications) remain open and are
  disclosed in the manuscript.
