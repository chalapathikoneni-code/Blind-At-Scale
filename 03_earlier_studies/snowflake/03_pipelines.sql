-- =====================================================================
-- Blind at Scale — Snowflake arm pipelines (S-XENG / S-SIZE)
-- File 3 of 4: the 9 pipelines as Snowflake SQL.
--
-- Run with: sf_driver.py run-sql 03_pipelines.sql --arm-tag sxeng \
--               --run-tag 20261125
-- (S-SIZE legs use --arm-tag ssize_xs / ssize_s / ssize_m with the
--  matching warehouse: sf_driver.py --warehouse BAS_SF_WH_S etc.)
--
-- PLACEHOLDERS: ARM_TAG and RUN_TAG are substituted by sf_driver.py
-- (--arm-tag / --run-tag). The resulting QUERY_TAG is
--   bas_<arm>_<pipeline>_<run_tag>
-- e.g. bas_sxeng_G_A_20261125.
--
-- STRUCTURE per pipeline:
--   1. ALTER SESSION SET QUERY_TAG (tags every following statement
--      until the next ALTER SESSION).
--   2. CREATE OR REPLACE TABLE RUN.RES_<pid> — the per-pipeline result
--      capture table (fresh BAS_* object; OR REPLACE is safe).
--   3. ONE INSERT ... SELECT statement — THE pipeline query. It is a
--      single statement so the driver can time it individually and so
--      QUERY_HISTORY attributes exactly one QUERY_ID (QUERY_TYPE =
--      'INSERT') to the pipeline. 04_profiles.sql filters to it.
--      Metric branches (UNION ALL) may re-execute the pipeline's
--      aggregation; that re-execution is part of the pipeline's
--      measured cost and is documented per pipeline below.
-- Result tables: (PIPELINE, METRIC, VALUE, NOTE, MEASURED_AT).
--
-- VERDICTS are NOT assigned here. The RES tables hold EXECUTED
-- MEASUREMENTS (data-level verification: Snowflake-computed counts
-- must equal the local GT values exactly). Verdicts come from:
--   - 04_profiles.sql: the pre-registered remote-spill proxy
--     (G/J/W pipelines), and
--   - collect_sf.py: the D4 analytic splitmix64 load vs T (F pipelines).
--
-- F PIPELINES (D4 mirror): the raw join is NOT executed. The capture
-- query computes the exact output size first (SUM of per-key products)
-- and records raw_join_status = SKIPPED_NOT_MATERIALISABLE via a
-- data-driven CASE when the exact size exceeds 2e8 rows (F_A exact:
-- 898,098,021,014 rows). If the exact size ever fit under the cap, the
-- status would read UNDER_CAP_RUN_RAW_MANUALLY — the raw join is never
-- executed by this script. The analytic splitmix64 load is computed
-- LOCALLY in collect_sf.py from the per-key product vector exported
-- below (COPY ... TO the stage) — exactly as the pod driver computed
-- it from Spark-produced counts. splitmix64 in SQL is error-prone and
-- is deliberately NOT attempted here.
--
-- WHAT SNOWFLAKE CANNOT SHOW (PROGRAM_DESIGN.md §4.5): Snowflake has no
-- user-visible shuffle partitions and no Spark-style shuffle-read
-- metric. These queries therefore measure exact result counts (data
-- level) while elapsed/spill come from QUERY_HISTORY (cost level).
-- =====================================================================

USE DATABASE BLINDATSCALE_SF;
USE SCHEMA RUN;
-- Warehouse is selected by the driver (--warehouse; default BAS_SF_WH_XS).

-- ============================ G_A ============================
ALTER SESSION SET QUERY_TAG = 'bas_ARM_TAG_G_A_RUN_TAG';
CREATE OR REPLACE TABLE BLINDATSCALE_SF.RUN.RES_G_A (
  PIPELINE VARCHAR, METRIC VARCHAR, VALUE NUMBER, NOTE VARCHAR,
  MEASURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
INSERT INTO BLINDATSCALE_SF.RUN.RES_G_A (PIPELINE, METRIC, VALUE, NOTE)
  SELECT 'G_A', 'max_group_count', MAX(c),
         'max COUNT(*) per kA (GROUP BY executed once, in this branch)'
  FROM (SELECT kA, COUNT(*) AS c FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kA)
  UNION ALL
  SELECT 'G_A', 'total_rows', COUNT(*), 'COUNT(*) of FACT'
  FROM BLINDATSCALE_SF.RUN.FACT
  UNION ALL
  SELECT 'G_A', 'distinct_keys', COUNT(DISTINCT kA), 'COUNT(DISTINCT kA)'
  FROM BLINDATSCALE_SF.RUN.FACT;

-- ============================ G_B ============================
ALTER SESSION SET QUERY_TAG = 'bas_ARM_TAG_G_B_RUN_TAG';
CREATE OR REPLACE TABLE BLINDATSCALE_SF.RUN.RES_G_B (
  PIPELINE VARCHAR, METRIC VARCHAR, VALUE NUMBER, NOTE VARCHAR,
  MEASURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
INSERT INTO BLINDATSCALE_SF.RUN.RES_G_B (PIPELINE, METRIC, VALUE, NOTE)
  SELECT 'G_B', 'max_group_count', MAX(c),
         'max COUNT(*) per kB (GROUP BY executed once, in this branch)'
  FROM (SELECT kB, COUNT(*) AS c FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kB)
  UNION ALL
  SELECT 'G_B', 'total_rows', COUNT(*), 'COUNT(*) of FACT'
  FROM BLINDATSCALE_SF.RUN.FACT
  UNION ALL
  SELECT 'G_B', 'distinct_keys', COUNT(DISTINCT kB), 'COUNT(DISTINCT kB)'
  FROM BLINDATSCALE_SF.RUN.FACT;

-- ============================ G_N ============================
ALTER SESSION SET QUERY_TAG = 'bas_ARM_TAG_G_N_RUN_TAG';
CREATE OR REPLACE TABLE BLINDATSCALE_SF.RUN.RES_G_N (
  PIPELINE VARCHAR, METRIC VARCHAR, VALUE NUMBER, NOTE VARCHAR,
  MEASURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
INSERT INTO BLINDATSCALE_SF.RUN.RES_G_N (PIPELINE, METRIC, VALUE, NOTE)
  SELECT 'G_N', 'max_group_count', MAX(c),
         'max COUNT(*) per kN, NULL sentinel -1 EXCLUDED (mirrors the local G_N masking)'
  FROM (SELECT kN, COUNT(*) AS c FROM BLINDATSCALE_SF.RUN.FACT
        WHERE kN <> -1 GROUP BY kN)
  UNION ALL
  SELECT 'G_N', 'null_group_count', COUNT(*), 'COUNT(*) of kN = -1 rows'
  FROM BLINDATSCALE_SF.RUN.FACT WHERE kN = -1
  UNION ALL
  SELECT 'G_N', 'total_rows', COUNT(*), 'COUNT(*) of FACT'
  FROM BLINDATSCALE_SF.RUN.FACT;

-- ============================ J_A ============================
ALTER SESSION SET QUERY_TAG = 'bas_ARM_TAG_J_A_RUN_TAG';
CREATE OR REPLACE TABLE BLINDATSCALE_SF.RUN.RES_J_A (
  PIPELINE VARCHAR, METRIC VARCHAR, VALUE NUMBER, NOTE VARCHAR,
  MEASURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
-- NOTE: the fact-dim join is executed twice (once per metric branch);
-- both executions are inside the single timed INSERT statement.
INSERT INTO BLINDATSCALE_SF.RUN.RES_J_A (PIPELINE, METRIC, VALUE, NOTE)
  SELECT 'J_A', 'join_output_rows', COUNT(*), 'COUNT(*) of fact JOIN dim on kA'
  FROM BLINDATSCALE_SF.RUN.FACT f
  JOIN BLINDATSCALE_SF.RUN.DIM d ON f.kA = d.dim_key
  UNION ALL
  SELECT 'J_A', 'max_per_key_output', MAX(c),
         'max per-key join output rows (grouped join)'
  FROM (SELECT f.kA, COUNT(*) AS c FROM BLINDATSCALE_SF.RUN.FACT f
        JOIN BLINDATSCALE_SF.RUN.DIM d ON f.kA = d.dim_key GROUP BY f.kA);

-- ============================ J_B ============================
ALTER SESSION SET QUERY_TAG = 'bas_ARM_TAG_J_B_RUN_TAG';
CREATE OR REPLACE TABLE BLINDATSCALE_SF.RUN.RES_J_B (
  PIPELINE VARCHAR, METRIC VARCHAR, VALUE NUMBER, NOTE VARCHAR,
  MEASURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
-- NOTE: the fact-dim join is executed twice (once per metric branch);
-- both executions are inside the single timed INSERT statement.
INSERT INTO BLINDATSCALE_SF.RUN.RES_J_B (PIPELINE, METRIC, VALUE, NOTE)
  SELECT 'J_B', 'join_output_rows', COUNT(*), 'COUNT(*) of fact JOIN dim on kB'
  FROM BLINDATSCALE_SF.RUN.FACT f
  JOIN BLINDATSCALE_SF.RUN.DIM d ON f.kB = d.dim_key
  UNION ALL
  SELECT 'J_B', 'max_per_key_output', MAX(c),
         'max per-key join output rows (grouped join)'
  FROM (SELECT f.kB, COUNT(*) AS c FROM BLINDATSCALE_SF.RUN.FACT f
        JOIN BLINDATSCALE_SF.RUN.DIM d ON f.kB = d.dim_key GROUP BY f.kB);

-- ============================ J_N ============================
ALTER SESSION SET QUERY_TAG = 'bas_ARM_TAG_J_N_RUN_TAG';
CREATE OR REPLACE TABLE BLINDATSCALE_SF.RUN.RES_J_N (
  PIPELINE VARCHAR, METRIC VARCHAR, VALUE NUMBER, NOTE VARCHAR,
  MEASURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
-- NOTE: the fact-dim join is executed twice (once per metric branch);
-- both executions are inside the single timed INSERT statement.
-- kN = -1 rows match no dim key and drop out (mirrors the local J_N
-- masking of the NULL group).
INSERT INTO BLINDATSCALE_SF.RUN.RES_J_N (PIPELINE, METRIC, VALUE, NOTE)
  SELECT 'J_N', 'join_output_rows', COUNT(*), 'COUNT(*) of fact JOIN dim on kN'
  FROM BLINDATSCALE_SF.RUN.FACT f
  JOIN BLINDATSCALE_SF.RUN.DIM d ON f.kN = d.dim_key
  UNION ALL
  SELECT 'J_N', 'max_per_key_output', MAX(c),
         'max per-key join output rows (grouped join)'
  FROM (SELECT f.kN, COUNT(*) AS c FROM BLINDATSCALE_SF.RUN.FACT f
        JOIN BLINDATSCALE_SF.RUN.DIM d ON f.kN = d.dim_key GROUP BY f.kN);

-- ============================ F_A ============================
ALTER SESSION SET QUERY_TAG = 'bas_ARM_TAG_F_A_RUN_TAG';
CREATE OR REPLACE TABLE BLINDATSCALE_SF.RUN.RES_F_A (
  PIPELINE VARCHAR, METRIC VARCHAR, VALUE NUMBER, NOTE VARCHAR,
  MEASURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
-- Aggregate form ONLY (D4 mirror): per-key counts of both facts, join of
-- the two 20,000-row count tables, per-key products. The raw join is NOT
-- executed; the guard CASE records SKIPPED_NOT_MATERIALISABLE when the
-- exact output size exceeds 2e8 rows (F_A exact: 898,098,021,014).
INSERT INTO BLINDATSCALE_SF.RUN.RES_F_A (PIPELINE, METRIC, VALUE, NOTE)
  SELECT 'F_A', 'distinct_shared_keys', COUNT(*),
         'keys present in both facts (count-table join size)'
  FROM (SELECT f.kA FROM (SELECT kA, COUNT(*) AS c1 FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kA) f
        JOIN (SELECT kA, COUNT(*) AS c2 FROM BLINDATSCALE_SF.RUN.FACT2 GROUP BY kA) g
        ON f.kA = g.kA)
  UNION ALL
  SELECT 'F_A', 'exact_join_output_rows', SUM(c1 * c2),
         'SUM of per-key products = exact raw join output size'
  FROM (SELECT f.kA, f.c1, g.c2
        FROM (SELECT kA, COUNT(*) AS c1 FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kA) f
        JOIN (SELECT kA, COUNT(*) AS c2 FROM BLINDATSCALE_SF.RUN.FACT2 GROUP BY kA) g
        ON f.kA = g.kA)
  UNION ALL
  SELECT 'F_A', 'max_per_key_product', MAX(c1 * c2),
         'hottest single key contribution to the join output'
  FROM (SELECT f.kA, f.c1, g.c2
        FROM (SELECT kA, COUNT(*) AS c1 FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kA) f
        JOIN (SELECT kA, COUNT(*) AS c2 FROM BLINDATSCALE_SF.RUN.FACT2 GROUP BY kA) g
        ON f.kA = g.kA)
  UNION ALL
  SELECT 'F_A', 'raw_join_status', exact_total,
         CASE WHEN exact_total > 200000000 THEN 'SKIPPED_NOT_MATERIALISABLE'
              ELSE 'UNDER_CAP_RUN_RAW_MANUALLY' END
  FROM (SELECT SUM(c1 * c2) AS exact_total
        FROM (SELECT f.kA, f.c1, g.c2
              FROM (SELECT kA, COUNT(*) AS c1 FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kA) f
              JOIN (SELECT kA, COUNT(*) AS c2 FROM BLINDATSCALE_SF.RUN.FACT2 GROUP BY kA) g
              ON f.kA = g.kA));
-- Per-key product vector export for the LOCAL analytic load computation
-- (collect_sf.py; D4 mirror — the analytic formula runs locally on
-- Snowflake-produced counts, exactly as the pod driver did).
COPY INTO @BLINDATSCALE_SF.UTIL.BAS_STAGE/f_products/F_A_products.csv
  FROM (
    SELECT f.kA AS K, f.c1 * g.c2 AS PROD
    FROM (SELECT kA, COUNT(*) AS c1 FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kA) f
    JOIN (SELECT kA, COUNT(*) AS c2 FROM BLINDATSCALE_SF.RUN.FACT2 GROUP BY kA) g
    ON f.kA = g.kA
  )
  FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'NONE')
  SINGLE = TRUE
  HEADER = TRUE
  OVERWRITE = TRUE;

-- ============================ F_B ============================
ALTER SESSION SET QUERY_TAG = 'bas_ARM_TAG_F_B_RUN_TAG';
CREATE OR REPLACE TABLE BLINDATSCALE_SF.RUN.RES_F_B (
  PIPELINE VARCHAR, METRIC VARCHAR, VALUE NUMBER, NOTE VARCHAR,
  MEASURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
-- Aggregate form ONLY (D4 mirror); raw join NOT executed; guard CASE as F_A.
INSERT INTO BLINDATSCALE_SF.RUN.RES_F_B (PIPELINE, METRIC, VALUE, NOTE)
  SELECT 'F_B', 'distinct_shared_keys', COUNT(*),
         'keys present in both facts (count-table join size)'
  FROM (SELECT f.kB FROM (SELECT kB, COUNT(*) AS c1 FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kB) f
        JOIN (SELECT kB, COUNT(*) AS c2 FROM BLINDATSCALE_SF.RUN.FACT2 GROUP BY kB) g
        ON f.kB = g.kB)
  UNION ALL
  SELECT 'F_B', 'exact_join_output_rows', SUM(c1 * c2),
         'SUM of per-key products = exact raw join output size'
  FROM (SELECT f.kB, f.c1, g.c2
        FROM (SELECT kB, COUNT(*) AS c1 FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kB) f
        JOIN (SELECT kB, COUNT(*) AS c2 FROM BLINDATSCALE_SF.RUN.FACT2 GROUP BY kB) g
        ON f.kB = g.kB)
  UNION ALL
  SELECT 'F_B', 'max_per_key_product', MAX(c1 * c2),
         'hottest single key contribution to the join output'
  FROM (SELECT f.kB, f.c1, g.c2
        FROM (SELECT kB, COUNT(*) AS c1 FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kB) f
        JOIN (SELECT kB, COUNT(*) AS c2 FROM BLINDATSCALE_SF.RUN.FACT2 GROUP BY kB) g
        ON f.kB = g.kB)
  UNION ALL
  SELECT 'F_B', 'raw_join_status', exact_total,
         CASE WHEN exact_total > 200000000 THEN 'SKIPPED_NOT_MATERIALISABLE'
              ELSE 'UNDER_CAP_RUN_RAW_MANUALLY' END
  FROM (SELECT SUM(c1 * c2) AS exact_total
        FROM (SELECT f.kB, f.c1, g.c2
              FROM (SELECT kB, COUNT(*) AS c1 FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kB) f
              JOIN (SELECT kB, COUNT(*) AS c2 FROM BLINDATSCALE_SF.RUN.FACT2 GROUP BY kB) g
              ON f.kB = g.kB));
-- Per-key product vector export for the LOCAL analytic load computation
-- (collect_sf.py; D4 mirror).
COPY INTO @BLINDATSCALE_SF.UTIL.BAS_STAGE/f_products/F_B_products.csv
  FROM (
    SELECT f.kB AS K, f.c1 * g.c2 AS PROD
    FROM (SELECT kB, COUNT(*) AS c1 FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kB) f
    JOIN (SELECT kB, COUNT(*) AS c2 FROM BLINDATSCALE_SF.RUN.FACT2 GROUP BY kB) g
    ON f.kB = g.kB
  )
  FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'NONE')
  SINGLE = TRUE
  HEADER = TRUE
  OVERWRITE = TRUE;

-- ============================ W_A ============================
ALTER SESSION SET QUERY_TAG = 'bas_ARM_TAG_W_A_RUN_TAG';
CREATE OR REPLACE TABLE BLINDATSCALE_SF.RUN.RES_W_A (
  PIPELINE VARCHAR, METRIC VARCHAR, VALUE NUMBER, NOTE VARCHAR,
  MEASURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
INSERT INTO BLINDATSCALE_SF.RUN.RES_W_A (PIPELINE, METRIC, VALUE, NOTE)
  SELECT 'W_A', 'max_window_partition_rows', MAX(rn),
         'MAX(ROW_NUMBER() OVER (PARTITION BY kA ORDER BY s)) = largest kA group'
  FROM (SELECT ROW_NUMBER() OVER (PARTITION BY kA ORDER BY s) AS rn
        FROM BLINDATSCALE_SF.RUN.FACT);
