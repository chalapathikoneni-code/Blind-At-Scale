-- =====================================================================
-- Blind at Scale — Snowflake arm load (S-XENG / S-SIZE)
-- File 2 of 4: COPY the uploaded parquet into FACT/FACT2/DIM, verify,
-- materialise per-key counts, export the counts for the SHA check.
--
-- PREREQUISITE — upload the local parquet to the stage FIRST (one-time,
-- ~218 MB; ingress is free). From the machine holding the local data,
-- with the Snowflake key pair (sf_driver.py put), or from SnowSQL:
--
--   sf_driver.py put ~/workspace/blind-at-scale/data/zipf_s101/fact.parquet \
--       @BLINDATSCALE_SF.UTIL.BAS_STAGE/fact.parquet
--   sf_driver.py put ~/workspace/blind-at-scale/data/zipf_s101/fact2.parquet \
--       @BLINDATSCALE_SF.UTIL.BAS_STAGE/fact2.parquet
--   sf_driver.py put ~/workspace/blind-at-scale/data/zipf_s101/dim.parquet \
--       @BLINDATSCALE_SF.UTIL.BAS_STAGE/dim.parquet
--
-- (SnowSQL equivalent: PUT file:///local/path/fact.parquet
--  @BLINDATSCALE_SF.UTIL.BAS_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;)
--
-- Run with: sf_driver.py run-sql 02_load.sql
-- (default warehouse BAS_SF_WH_XS; the COPY is minutes of XS time.)
--
-- WHY UPLOAD (PROGRAM_DESIGN.md §6): Snowflake SQL cannot reproduce
-- numpy's multinomial draws, so regeneration in SQL would produce a
-- DIFFERENT dataset. Uploading the exact local files preserves
-- bit-identity. Verification is full-strength: per-key counts are
-- re-derived in Snowflake, exported (~1 MB CSV), and SHA-256-checked
-- locally against f9f3fa8b...28f by collect_sf.py, plus the max-count
-- checks below.
-- =====================================================================

USE DATABASE BLINDATSCALE_SF;
USE SCHEMA RUN;
USE WAREHOUSE BAS_SF_WH_XS;

-- ---- COPY (idempotent re-runs: truncate first — these are fresh BAS_* tables)
TRUNCATE TABLE IF EXISTS BLINDATSCALE_SF.RUN.FACT;
TRUNCATE TABLE IF EXISTS BLINDATSCALE_SF.RUN.FACT2;
TRUNCATE TABLE IF EXISTS BLINDATSCALE_SF.RUN.DIM;

COPY INTO BLINDATSCALE_SF.RUN.FACT
  FROM @BLINDATSCALE_SF.UTIL.BAS_STAGE/fact.parquet
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';

COPY INTO BLINDATSCALE_SF.RUN.FACT2
  FROM @BLINDATSCALE_SF.UTIL.BAS_STAGE/fact2.parquet
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';

COPY INTO BLINDATSCALE_SF.RUN.DIM
  FROM @BLINDATSCALE_SF.UTIL.BAS_STAGE/dim.parquet
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';

-- ---- verification: row counts, null count, max per-key counts ----
-- Each query returns an OK/MISMATCH check column so the driver log shows
-- pass/fail inline. Expected values are the local ground truth
-- (zipf seed 101; see gen_on_pod.py / PROGRAM_DESIGN.md §3).
SELECT 'FACT row count' AS CHECK_NAME, COUNT(*) AS ACTUAL, 10000000 AS EXPECTED,
       CASE WHEN COUNT(*) = 10000000 THEN 'OK' ELSE 'MISMATCH' END AS CHECK
FROM BLINDATSCALE_SF.RUN.FACT;

SELECT 'FACT2 row count' AS CHECK_NAME, COUNT(*) AS ACTUAL, 6000000 AS EXPECTED,
       CASE WHEN COUNT(*) = 6000000 THEN 'OK' ELSE 'MISMATCH' END AS CHECK
FROM BLINDATSCALE_SF.RUN.FACT2;

SELECT 'DIM row count' AS CHECK_NAME, COUNT(*) AS ACTUAL, 20000 AS EXPECTED,
       CASE WHEN COUNT(*) = 20000 THEN 'OK' ELSE 'MISMATCH' END AS CHECK
FROM BLINDATSCALE_SF.RUN.DIM;

SELECT 'kN null (-1) count' AS CHECK_NAME, COUNT(*) AS ACTUAL, 400000 AS EXPECTED,
       CASE WHEN COUNT(*) = 400000 THEN 'OK' ELSE 'MISMATCH' END AS CHECK
FROM BLINDATSCALE_SF.RUN.FACT WHERE kN = -1;

SELECT 'fact kA max' AS CHECK_NAME, MAX(c) AS ACTUAL, 953047 AS EXPECTED,
       CASE WHEN MAX(c) = 953047 THEN 'OK' ELSE 'MISMATCH' END AS CHECK
FROM (SELECT kA, COUNT(*) AS c FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kA);

SELECT 'fact kB max' AS CHECK_NAME, MAX(c) AS ACTUAL, 77009 AS EXPECTED,
       CASE WHEN MAX(c) = 77009 THEN 'OK' ELSE 'MISMATCH' END AS CHECK
FROM (SELECT kB, COUNT(*) AS c FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kB);

SELECT 'fact kC max' AS CHECK_NAME, MAX(c) AS ACTUAL, 584 AS EXPECTED,
       CASE WHEN MAX(c) = 584 THEN 'OK' ELSE 'MISMATCH' END AS CHECK
FROM (SELECT kC, COUNT(*) AS c FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kC);

SELECT 'fact kN max (non-null)' AS CHECK_NAME, MAX(c) AS ACTUAL, 916064 AS EXPECTED,
       CASE WHEN MAX(c) = 916064 THEN 'OK' ELSE 'MISMATCH' END AS CHECK
FROM (SELECT kN, COUNT(*) AS c FROM BLINDATSCALE_SF.RUN.FACT
      WHERE kN <> -1 GROUP BY kN);

-- ---- materialise per-key counts (for the counts-export SHA check) ----
TRUNCATE TABLE IF EXISTS BLINDATSCALE_SF.RUN.FACT_COUNTS;
TRUNCATE TABLE IF EXISTS BLINDATSCALE_SF.RUN.FACT2_COUNTS;

INSERT INTO BLINDATSCALE_SF.RUN.FACT_COUNTS (COL, K, CNT)
  SELECT 'kA', kA, COUNT(*) FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kA;
INSERT INTO BLINDATSCALE_SF.RUN.FACT_COUNTS (COL, K, CNT)
  SELECT 'kB', kB, COUNT(*) FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kB;
INSERT INTO BLINDATSCALE_SF.RUN.FACT_COUNTS (COL, K, CNT)
  SELECT 'kC', kC, COUNT(*) FROM BLINDATSCALE_SF.RUN.FACT GROUP BY kC;
INSERT INTO BLINDATSCALE_SF.RUN.FACT_COUNTS (COL, K, CNT)
  SELECT 'kN', kN, COUNT(*) FROM BLINDATSCALE_SF.RUN.FACT
  WHERE kN <> -1 GROUP BY kN;
INSERT INTO BLINDATSCALE_SF.RUN.FACT2_COUNTS (COL, K, CNT)
  SELECT 'kA', kA, COUNT(*) FROM BLINDATSCALE_SF.RUN.FACT2 GROUP BY kA;
INSERT INTO BLINDATSCALE_SF.RUN.FACT2_COUNTS (COL, K, CNT)
  SELECT 'kB', kB, COUNT(*) FROM BLINDATSCALE_SF.RUN.FACT2 GROUP BY kB;

-- ---- counts export: the six per-key count vectors as one CSV ----
-- Downloaded by the operator (sf_driver.py get-counts) and SHA-256-checked
-- locally by collect_sf.py against f9f3fa8b...28f (same vector order as
-- the local counts.npz: fact kA,kB,kC,kN, fact2 kA,kB).
-- NOTE: keys with zero count have no row in *_COUNTS; collect_sf.py
-- rebuilds the full 20,000-length vectors with zeros for missing keys.
COPY INTO @BLINDATSCALE_SF.UTIL.BAS_STAGE/counts/counts_export.csv
  FROM (
    SELECT 'fact_kA' AS VEC, K, CNT FROM BLINDATSCALE_SF.RUN.FACT_COUNTS WHERE COL = 'kA'
    UNION ALL
    SELECT 'fact_kB' AS VEC, K, CNT FROM BLINDATSCALE_SF.RUN.FACT_COUNTS WHERE COL = 'kB'
    UNION ALL
    SELECT 'fact_kC' AS VEC, K, CNT FROM BLINDATSCALE_SF.RUN.FACT_COUNTS WHERE COL = 'kC'
    UNION ALL
    SELECT 'fact_kN' AS VEC, K, CNT FROM BLINDATSCALE_SF.RUN.FACT_COUNTS WHERE COL = 'kN'
    UNION ALL
    SELECT 'fact2_kA' AS VEC, K, CNT FROM BLINDATSCALE_SF.RUN.FACT2_COUNTS WHERE COL = 'kA'
    UNION ALL
    SELECT 'fact2_kB' AS VEC, K, CNT FROM BLINDATSCALE_SF.RUN.FACT2_COUNTS WHERE COL = 'kB'
  )
  FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'NONE')
  SINGLE = TRUE
  HEADER = TRUE
  OVERWRITE = TRUE;
