-- =====================================================================
-- Blind at Scale — Snowflake arm profiles (S-XENG / S-SIZE)
-- File 4 of 4: extract per-pipeline profiles from QUERY_HISTORY,
-- apply the pre-registered failure proxy, capture warehouse metering.
--
-- Run with: sf_driver.py profile
-- (runs this file and saves the SELECT results as JSON).
--
-- LATENCY (PROGRAM_DESIGN.md §9 Q6): SNOWFLAKE.ACCOUNT_USAGE views lag
-- behind execution by up to ~3 hours; INFORMATION_SCHEMA.QUERY_HISTORY
-- is near-real-time but retains only 7 days and is scoped to the
-- current user/role. The canonical source is ACCOUNT_USAGE (query 1);
-- if its rows are not yet visible, the driver retries after a
-- documented delay and falls back to INFORMATION_SCHEMA (query 2).
-- If BOTH fail to return a pipeline's profile, the arm is VOID
-- (PROGRAM_DESIGN.md §4.5) — profiles are never estimated.
--
-- RUN WINDOW: BAS_RUN_START defaults to 7 days ago; narrow it for a
-- specific run by editing the SET below (or by passing a tighter
-- window when invoking the driver — see sf_driver.py --run-start).
-- =====================================================================

SET BAS_RUN_START = DATEADD(day, -7, CURRENT_TIMESTAMP());

-- ==================== Query 1 (canonical): ACCOUNT_USAGE ====================
-- One row per pipeline per warehouse size (latest execution wins via
-- QUALIFY; re-runs are visible in the audit query below). Filtered to
-- QUERY_TYPE = 'INSERT': each pipeline's timed statement is its single
-- INSERT ... SELECT (03_pipelines.sql). CREATE TABLE / COPY statements
-- carrying the same tag are excluded here and listed in the audit query.
SELECT
  REPLACE(REGEXP_SUBSTR(QUERY_TEXT, 'RES_[A-Z]_[A-Z]', 1, 1), 'RES_', '') AS PIPELINE,
  -- Arm is the 2nd tag part; S-SIZE leg tags (ssize_xs/...) contain an
  -- underscore, so this yields 'ssize' for all S-SIZE legs — the
  -- warehouse name disambiguates the leg (see WAREHOUSE_NAME).
  SPLIT_PART(QUERY_TAG, '_', 2) AS ARM,
  SPLIT_PART(QUERY_TAG, '_', -1) AS RUN_LABEL,
  QUERY_TAG,
  QUERY_ID,
  LEFT(QUERY_TEXT, 200) AS QUERY_TEXT_200,
  START_TIME,
  END_TIME,
  TOTAL_ELAPSED_TIME,
  BYTES_SCANNED,
  BYTES_SPILLED_TO_LOCAL_STORAGE,
  BYTES_SPILLED_TO_REMOTE_STORAGE,
  PARTITIONS_SCANNED,
  PARTITIONS_TOTAL,
  ROWS_PRODUCED,
  ERROR_CODE,
  ERROR_MESSAGE,
  WAREHOUSE_NAME,
  WAREHOUSE_SIZE,
  -- ================================================================
  -- PRE-REGISTERED SNOWFLAKE FAILURE PROXY (PROGRAM_DESIGN.md §4.5):
  -- a pipeline's Snowflake verdict is FAIL iff it spilled to REMOTE
  -- storage (BYTES_SPILLED_TO_REMOTE_STORAGE > 0) OR the query errored.
  -- Local-disk spill is recorded but is NOT verdict-bearing (common
  -- even for healthy queries on XS). This proxy was fixed BEFORE any
  -- cloud run and must NOT be re-fitted after seeing results. If no
  -- pipeline spills remotely, all proxy verdicts are PASS and the
  -- verdict comparison is vacuous — pre-registered as "proxy
  -- uninformative at this scale/warehouse"; the arm's evidence is
  -- then the rank-correlation analysis plus the elapsed comparison.
  -- Manuscript rule: Snowflake verdicts are labelled spill-proxy
  -- verdicts, never the study's load verdicts.
  -- ================================================================
  CASE WHEN BYTES_SPILLED_TO_REMOTE_STORAGE > 0 OR ERROR_CODE IS NOT NULL
       THEN 'FAIL' ELSE 'PASS' END AS SNOWFLAKE_VERDICT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE STARTSWITH(QUERY_TAG, 'bas_')
  AND START_TIME >= $BAS_RUN_START
  AND QUERY_TYPE = 'INSERT'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY REPLACE(REGEXP_SUBSTR(QUERY_TEXT, 'RES_[A-Z]_[A-Z]', 1, 1), 'RES_', ''),
               WAREHOUSE_NAME
  ORDER BY START_TIME DESC
) = 1
ORDER BY PIPELINE, WAREHOUSE_NAME;

-- ==================== Query 2 (fallback): INFORMATION_SCHEMA ====================
-- Same shape as query 1, low-latency source. Use when ACCOUNT_USAGE
-- rows are not yet visible. Retention 7 days; current-user/role scope.
SELECT
  REPLACE(REGEXP_SUBSTR(QUERY_TEXT, 'RES_[A-Z]_[A-Z]', 1, 1), 'RES_', '') AS PIPELINE,
  SPLIT_PART(QUERY_TAG, '_', 2) AS ARM,
  SPLIT_PART(QUERY_TAG, '_', -1) AS RUN_LABEL,
  QUERY_TAG,
  QUERY_ID,
  LEFT(QUERY_TEXT, 200) AS QUERY_TEXT_200,
  START_TIME,
  END_TIME,
  TOTAL_ELAPSED_TIME,
  BYTES_SCANNED,
  BYTES_SPILLED_TO_LOCAL_STORAGE,
  BYTES_SPILLED_TO_REMOTE_STORAGE,
  PARTITIONS_SCANNED,
  PARTITIONS_TOTAL,
  ROWS_PRODUCED,
  ERROR_CODE,
  ERROR_MESSAGE,
  WAREHOUSE_NAME,
  WAREHOUSE_SIZE,
  CASE WHEN BYTES_SPILLED_TO_REMOTE_STORAGE > 0 OR ERROR_CODE IS NOT NULL
       THEN 'FAIL' ELSE 'PASS' END AS SNOWFLAKE_VERDICT
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
  END_TIME_RANGE_START => $BAS_RUN_START,
  END_TIME_RANGE_END => CURRENT_TIMESTAMP(),
  RESULT_LIMIT => 10000))
WHERE STARTSWITH(QUERY_TAG, 'bas_')
  AND QUERY_TYPE = 'INSERT'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY REPLACE(REGEXP_SUBSTR(QUERY_TEXT, 'RES_[A-Z]_[A-Z]', 1, 1), 'RES_', ''),
               WAREHOUSE_NAME
  ORDER BY START_TIME DESC
) = 1
ORDER BY PIPELINE, WAREHOUSE_NAME;

-- ==================== Query 3: statement-level audit ====================
-- Every tagged statement (CREATE TABLE, INSERT, COPY) for the run window,
-- so re-runs, DDL, and exports are visible alongside the profiles.
SELECT
  QUERY_TAG,
  QUERY_ID,
  QUERY_TYPE,
  LEFT(QUERY_TEXT, 120) AS QUERY_TEXT_120,
  START_TIME,
  TOTAL_ELAPSED_TIME,
  ERROR_CODE,
  WAREHOUSE_NAME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE STARTSWITH(QUERY_TAG, 'bas_')
  AND START_TIME >= $BAS_RUN_START
ORDER BY START_TIME;

-- ==================== Query 4: warehouse metering (cost capture) ====================
-- Credits consumed by the BAS_SF_WH_* warehouses in the run window.
-- Cost column is a planning estimate at the Enterprise list price
-- ($3.00/credit); the recorded evidence is CREDITS_USED.
SELECT
  WAREHOUSE_NAME,
  SUM(CREDITS_USED) AS CREDITS_USED,
  SUM(CREDITS_USED) * 3.00 AS ESTIMATED_COST_USD_AT_3_PER_CREDIT,
  MIN(START_TIME) AS FIRST_USE,
  MAX(END_TIME) AS LAST_USE
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME IN ('BAS_SF_WH_XS', 'BAS_SF_WH_S', 'BAS_SF_WH_M')
  AND START_TIME >= $BAS_RUN_START
GROUP BY WAREHOUSE_NAME
ORDER BY WAREHOUSE_NAME;
