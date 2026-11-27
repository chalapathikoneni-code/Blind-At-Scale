OpenAlex self-join measurement - EM_Results delivery
Captured (UTC): 2026-11-26 01:33:45Z
Account <ACCOUNT_ID>, region us-east-1, cluster <CLUSTER_ID> (emr-7.14.0)

RESULTS (both executed via --force-measure; prediction recorded first; streaming per-partition count, no glom)
  Run 1 - 1-part selfjoin (data: Access/Final_1part)
    predicted max partition = 43,344,257 ; measured max partition = 43,340,706
    Sigma count^2 predicted = 262,464,138 ; measured output rows = 262,464,138
    elapsed 198s ; verdict FAIL/FAIL ; app <APPLICATION_ID>
  Run 2 - 4-part selfjoin (data: Access/Final, Step 1 prep tables)
    predicted max partition = 122,469,088 ; measured max partition = 123,158,197
    Sigma count^2 predicted = 795,242,814 ; measured output rows = 795,242,814
    elapsed 387s ; verdict FAIL/FAIL ; app <APPLICATION_ID>
  (full 128-element per-partition load array is inside each measured_json file)

CONTENTS
  measured_json/    the two result JSONs
  step_logs/        EMR step logs for both runs (controller/stderr/stdout)
  event_logs/       both Spark application event logs (physical evidence)
  patched_scripts/  e1_join.py (with --force-measure), e6_aqe.py, e1_more.py, common.py
