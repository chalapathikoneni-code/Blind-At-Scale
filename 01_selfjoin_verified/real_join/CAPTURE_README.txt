REAL (materialized) OpenAlex self-join - Blind at Scale - EM_Results delivery
Captured (UTC): 2026-11-26 07:10:26Z
Account <ACCOUNT_ID>, region us-east-1
Cluster <CLUSTER_ID>, emr-7.14.0 (Spark 3.5.8-amzn-0)
Frozen config: AQE OFF, spark.sql.shuffle.partitions=128, 18 executors,
executor-memory 8g / executor-cores 4 / driver-memory 8g,
default sort-merge join (spark.sql.autoBroadcastJoinThreshold=-1),
spark.yarn.maxAppAttempts=1. Identical to the Nov 25 counter steps.

PURPOSE
Answers the reviewer objection that the Nov 25 runs used a streaming
per-partition counter and "measured the load distribution without
executing the join." This run executes the ACTUAL self-join and writes
the joined rows to S3 Parquet (no count_per_partition, no glom, no
counting shortcut for the join). One attempt each; 4-part run FIRST.

RESULT - both runs COMPLETED, materialized, 0 failed tasks, 0 spill.

 4-part (flagship)  app <APPLICATION_ID>  step <STEP_ID>
   input Access/Final (left rows 5,368,834)
   predicted: total 795,242,814 ; hottest partition 122,469,088 ; FAIL/FAIL
   MEASURED : total 795,242,814 (exact) ; wall-clock 112.1 s
   join+write stage 7: 128 tasks, 0 failed, output_records max 123,158,197
       median 1,519,986 sum 795,242,814 ; output 3,477,379,546 bytes
       shuffle read records sum 10,737,668 (max 103,922) ; mem/disk spill 0
   join exchange stage 6: 44 tasks, shuffle write 109,310,564 bytes, 0 failed
   output parquet: s3://YOUR_BUCKET/Access/RealJoin/s4/joined.parquet (128 parts)

 1-part  app <APPLICATION_ID>  step <STEP_ID>
   input Access/Final_1part (left rows 1,260,852)
   predicted: total 262,464,138 ; hottest partition 43,344,257 ; FAIL/FAIL
   MEASURED : total 262,464,138 (exact) ; wall-clock 69.2 s
   join+write stage 7: 128 tasks, 0 failed, output_records max 43,340,706
       median 217,340 sum 262,464,138 ; output 438,037,502 bytes
       shuffle read records sum 2,521,704 (max 30,700) ; mem/disk spill 0
   join exchange stage 6: 13 tasks, shuffle write 25,134,303 bytes, 0 failed
   output parquet: s3://YOUR_BUCKET/Access/RealJoin/s1/joined.parquet

guard_overridden: true on both (force-measure); the prediction is recorded
in result_pre.json BEFORE each join launches and is unchanged. No threshold,
partitioning, or prediction code was modified.

CONTENTS
  result_json/     result_pre_{s4,s1}.json (predictions, pre-launch) and
                   result_{s4,s1}.json (final, with measured rows + elapsed)
  event_logs/      full Spark event logs for both applications (0016, 0017)
  step_logs/       EMR controller/stderr for both steps
  metrics/         per-stage task metrics parsed from each event log
  as_run_scripts/  e1_selfjoin_real.py (the modified join code that executed),
                   common.py
  step_commands/   exact spark-submit step command lines (JSON)
