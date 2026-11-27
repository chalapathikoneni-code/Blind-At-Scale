# Sanitization Record

This package was prepared from the author's working archive on
2026-11-26. Sanitization changed strings and configuration values
only. No logic, no result value, and no measured number was altered.
This file records the replacement rules, the per-file locations, and
the diff evidence, so a reader can reconstruct exactly how the
deposit differs from the as-run originals.

## Replacement rules

| Category | Original | Replacement |
|---|---|---|
| `aws_account_id` | The author's AWS account ID | `<ACCOUNT_ID>` |
| `s3_bucket_uri` | `s3://` / `s3a://` URI of the author's bucket | Same scheme with `YOUR_BUCKET` |
| `bucket_name` | The bucket name appearing bare | `YOUR_BUCKET` |
| `emr_cluster_id` | EMR cluster IDs (`j-...`) | `<CLUSTER_ID>` |
| `emr_step_id` | EMR step IDs (`s-...`) | `<STEP_ID>` |
| `yarn_application_id` | YARN application IDs (`application_...`) | `<APPLICATION_ID>` |
| `databricks_workspace_host` | The author's Databricks workspace host | `<WORKSPACE_HOST>` |
| `author_email` | The author's email (Databricks job-definition owner fields) | `<AUTHOR_EMAIL>` |
| `em_dash_in_code` | Em-dash characters in Python/shell comments, docstrings, and display labels | `--` (the author's code standard) |

Totals across the package: account ID 2, S3 bucket URIs 75, bare
bucket name 5, cluster IDs 131, step IDs 20, YARN application IDs
385, workspace host 2, author email 6, em-dashes in code 160.

One judgement call, recorded explicitly: `code/drivers/
e4_parse_logs.py` (package path
`02_campaign_e1_e7/code/drivers/e4_parse_logs.py`) listed two
specific event-log file names from the author's runs as example
inputs; those file names embed YARN application IDs and were
redacted by the `yarn_application_id` rule. Point the script at your
own event logs. No other file required interpretation.

## Logic preservation

Every verification-critical script was diffed against its original
after sanitization:

- `01_selfjoin_verified/real_join/code/common.py` and
  `e1_selfjoin_real.py`: **byte-identical** to the as-run scripts
  (zero diff hunks). Original md5: common.py
  `c5cd8d7f09fbfa7bde4daeae14b13af3`, e1_selfjoin_real.py
  `7bd9579f5473e0703be0ff90949d381a`.
- `01_selfjoin_verified/streaming_counter/code/` (`common.py`,
  `e1_join.py`, `e1_more.py`, `e6_aqe.py`): **byte-identical** to
  the patched scripts received with the capture. Their md5 hashes
  are the ones recorded in the measured self-join verification
  report: `c5cd8d7f09fbfa7bde4daeae14b13af3`,
  `eef0a4a6012ee9f3873ba8f34a51a849`,
  `950b873f352842ce04c4e10bbd6d6053`,
  `8e3c2546ffa97fd12d958b1b86e699c9`.
- Campaign package (`02_campaign_e1_e7/code/emr_package/code/`):
  the only hunks are the default S3 prefix strings in `a9_100m.py`,
  `e6_synth.py`, and a help string in `prep_openalex.py`
  (`s3_bucket_uri` rule). All other files byte-identical.
- Campaign drivers (`02_campaign_e1_e7/code/drivers/`):
  `e4_analyse.py` differs only in em-dashes inside plot-label
  strings; `e7_real_data.py` only in em-dashes inside docstrings;
  `e4_parse_logs.py` only in the two redacted example file names
  above. All other files byte-identical.
- All 68 Python files in the package pass `python3 -m py_compile`
  after sanitization.

Result JSONs, metrics, and step-command files were changed only by
the identifier rules above (bucket URIs and application IDs inside
string values); every numeric value is unchanged.

## Per-file locations

| Package file | Replacements |
|---|---|
| `01_selfjoin_verified/real_join/CAPTURE_README.txt` | aws_account_id x1, emr_cluster_id x1, emr_step_id x2, s3_bucket_uri x2, yarn_application_id x2 |
| `01_selfjoin_verified/real_join/metrics/metrics_s1.json` | yarn_application_id x2 |
| `01_selfjoin_verified/real_join/metrics/metrics_s4.json` | yarn_application_id x2 |
| `01_selfjoin_verified/real_join/results/result_s1.json` | s3_bucket_uri x1 |
| `01_selfjoin_verified/real_join/results/result_s4.json` | s3_bucket_uri x1 |
| `01_selfjoin_verified/real_join/step_commands/realjoin_step_commands.json` | s3_bucket_uri x10 |
| `01_selfjoin_verified/streaming_counter/CAPTURE_README.txt` | aws_account_id x1, emr_cluster_id x1, yarn_application_id x2 |
| `02_campaign_e1_e7/code/drivers/e4_analyse.py` | em_dash_in_code x4 |
| `02_campaign_e1_e7/code/drivers/e4_parse_logs.py` | yarn_application_id x2 |
| `02_campaign_e1_e7/code/drivers/e7_real_data.py` | em_dash_in_code x4 |
| `02_campaign_e1_e7/code/emr_package/code/a9_100m.py` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/code/emr_package/code/e6_synth.py` | s3_bucket_uri x1 |
| `02_campaign_e1_e7/code/emr_package/code/prep_openalex.py` | s3_bucket_uri x1 |
| `02_campaign_e1_e7/code/emr_package/docs/RUNBOOK.md` | s3_bucket_uri x13 |
| `02_campaign_e1_e7/code/emr_package/scripts/add_steps.sh` | bucket_name x1, s3_bucket_uri x1 |
| `02_campaign_e1_e7/code/emr_package/scripts/emr_create_cluster.sh` | bucket_name x1 |
| `02_campaign_e1_e7/code/emr_package/scripts/upload_to_s3.sh` | bucket_name x1 |
| `02_campaign_e1_e7/docs/ALL_NEW_RESULTS.csv` | emr_cluster_id x111, emr_step_id x2, yarn_application_id x12 |
| `02_campaign_e1_e7/docs/E1_REAL_JOIN_VALIDATION_REPORT.md` | emr_cluster_id x1, yarn_application_id x1 |
| `02_campaign_e1_e7/docs/E4_PHYSICAL_VALIDATION_REPORT.md` | yarn_application_id x2 |
| `02_campaign_e1_e7/docs/FINAL_EXECUTION_PROTOCOL.md` | yarn_application_id x2 |
| `02_campaign_e1_e7/results/final_capture_eventlog_summary.json` | yarn_application_id x10 |
| `02_campaign_e1_e7/results/final_capture_output/e6_off_join_dim.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/final_capture_output/e6_off_synth_J_A.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/final_capture_output/e6_off_synth_J_N.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/final_capture_output/e6_on_join_dim.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/final_capture_output/e6_on_synth_J_A.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/final_capture_output/e6_on_synth_J_N.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/final_return_output/e6_off_join_dim.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/final_return_output/e6_off_join_meta.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/final_return_output/e6_off_synth_J_A.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/final_return_output/e6_off_synth_J_N.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/final_return_output/e6_on_join_dim.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/final_return_output/e6_on_join_meta.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/final_return_output/e6_on_synth_J_A.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/final_return_output/e6_on_synth_J_N.json` | s3_bucket_uri x2 |
| `02_campaign_e1_e7/results/strengthening/e4_analysis_detail.json` | yarn_application_id x31 |
| `02_campaign_e1_e7/results/strengthening/e4_stage_aggregates_realdata.json` | yarn_application_id x254 |
| `02_campaign_e1_e7/results/strengthening/e4_stage_aggregates_synthetic.json` | yarn_application_id x36 |
| `02_campaign_e1_e7/results/strengthening/emr_final_capture_eventlog_summary.json` | yarn_application_id x10 |
| `03_earlier_studies/databricks/dbx_driver.py` | databricks_workspace_host x2, em_dash_in_code x4 |
| `03_earlier_studies/databricks/feedback_dbx.py` | em_dash_in_code x12 |
| `03_earlier_studies/databricks/gen_on_dbx.py` | em_dash_in_code x2 |
| `03_earlier_studies/databricks/job_defs/README_JOB_DEFS.md` | author_email x1 |
| `03_earlier_studies/databricks/job_defs/d_dbx.json` | author_email x1 |
| `03_earlier_studies/databricks/job_defs/d_fb.json` | author_email x1 |
| `03_earlier_studies/databricks/job_defs/d_neg.json` | author_email x1 |
| `03_earlier_studies/databricks/job_defs/d_scale_100m.json` | author_email x1 |
| `03_earlier_studies/databricks/job_defs/d_scale_40m.json` | author_email x1 |
| `03_earlier_studies/databricks/negfam_dbx.py` | em_dash_in_code x7 |
| `03_earlier_studies/databricks/spark_pipelines_dbx.py` | em_dash_in_code x10 |
| `03_earlier_studies/emr_synthetic_100m/VERIFICATION_REPORT.md` | bucket_name x1, emr_cluster_id x2, emr_step_id x4, s3_bucket_uri x4, yarn_application_id x3 |
| `03_earlier_studies/local_study/code/classical_arms.py` | em_dash_in_code x2 |
| `03_earlier_studies/local_study/code/feedback.py` | em_dash_in_code x1 |
| `03_earlier_studies/local_study/code/gen_dataset.py` | em_dash_in_code x1 |
| `03_earlier_studies/local_study/code/gen_near_threshold.py` | em_dash_in_code x1 |
| `03_earlier_studies/local_study/code/keyspace_run.py` | em_dash_in_code x1 |
| `03_earlier_studies/local_study/code/outeq_check.py` | em_dash_in_code x1 |
| `03_earlier_studies/local_study/code/score_exp_a.py` | em_dash_in_code x4 |
| `03_earlier_studies/local_study/code/score_nonaligned.py` | em_dash_in_code x11 |
| `03_earlier_studies/nyc_realdata/VERIFICATION_REPORT.md` | emr_cluster_id x8, emr_step_id x5, s3_bucket_uri x2, yarn_application_id x5 |
| `03_earlier_studies/nyc_realdata/dry_run_realdata.py` | em_dash_in_code x6 |
| `03_earlier_studies/nyc_realdata/replicate_nyc_jan.py` | em_dash_in_code x1 |
| `03_earlier_studies/pod/collect_results.py` | em_dash_in_code x5 |
| `03_earlier_studies/pod/gen_on_pod.py` | em_dash_in_code x3 |
| `03_earlier_studies/pod/run_pod.sh` | em_dash_in_code x3 |
| `03_earlier_studies/pod/setup.sh` | em_dash_in_code x3 |
| `03_earlier_studies/pod/spark_pipelines.py` | em_dash_in_code x16 |
| `03_earlier_studies/pod_multihost/collect_results.py` | em_dash_in_code x5 |
| `03_earlier_studies/pod_multihost/gen_on_pod.py` | em_dash_in_code x3 |
| `03_earlier_studies/pod_multihost/lib_multihost.sh` | em_dash_in_code x4 |
| `03_earlier_studies/pod_multihost/run_master.sh` | em_dash_in_code x5 |
| `03_earlier_studies/pod_multihost/run_worker.sh` | em_dash_in_code x1 |
| `03_earlier_studies/pod_multihost/selftest.sh` | em_dash_in_code x3 |
| `03_earlier_studies/pod_multihost/setup_multihost.sh` | em_dash_in_code x3 |
| `03_earlier_studies/pod_multihost/spark_pipelines.py` | em_dash_in_code x17 |
| `03_earlier_studies/snowflake/collect_sf.py` | em_dash_in_code x13 |
| `03_earlier_studies/snowflake/sf_driver.py` | em_dash_in_code x4 |
| `04_verification/EMR_FINAL_CAPTURE_VERIFICATION.md` | emr_step_id x3 |
| `04_verification/GAP_REGISTER.csv` | s3_bucket_uri x1 |
| `04_verification/REPRODUCIBILITY_CLOSURE_REPORT.md` | bucket_name x1, s3_bucket_uri x7 |
| `04_verification/UPDATED_CLAIM_TO_EVIDENCE_MATRIX.md` | emr_cluster_id x1 |
| `04_verification/UPDATED_NUMBER_PROVENANCE.md` | emr_cluster_id x2, yarn_application_id x3 |
| `04_verification/VERIFICATION_REPORT_FINAL_RETURN.md` | emr_cluster_id x2, yarn_application_id x2 |
| `04_verification/VERIFICATION_REPORT_REALJOIN.md` | emr_cluster_id x1, emr_step_id x2, s3_bucket_uri x1, yarn_application_id x2 |
| `04_verification/VERIFICATION_REPORT_SELFJOIN.md` | emr_cluster_id x1, emr_step_id x2, yarn_application_id x2 |
