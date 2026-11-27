-- =====================================================================
-- Blind at Scale — Snowflake arm setup (S-XENG / S-SIZE)
-- File 1 of 4: database, schemas, warehouses, stage, tables.
--
-- Run with: sf_driver.py run-sql 01_setup.sql --no-warehouse
-- (no warehouse exists yet at this point; DDL needs none).
--
-- ISOLATION: every object here is Blind at Scale's own (BLINDATSCALE_SF
-- / BAS_SF_WH_*). VISTA's Snowflake resources — database VERDICTBENCH_D2,
-- warehouse VB_D2_WH — are NEVER referenced. See
-- CREDIT_AND_INTERFERENCE_REPORT.md §2.
--
-- IDEMPOTENCY: all statements use CREATE ... IF NOT EXISTS (or OR REPLACE
-- where noted). OR REPLACE is safe here because these are fresh BAS_*
-- objects owned by this program; nothing else writes to them.
--
-- QUERY_TAG convention (used by 03_pipelines.sql and 04_profiles.sql):
--   bas_<arm>_<pipeline>_<run_tag>
-- e.g. bas_sxeng_G_A_20261125, bas_ssize_xs_J_B_20261125.
-- The tag is set per statement batch via ALTER SESSION SET QUERY_TAG;
-- sf_driver.py substitutes the ARM_TAG and RUN_TAG placeholders.
-- Profiles are pulled by tag in 04_profiles.sql.
--
-- ROLE: the scripts only need CREATE DATABASE / CREATE WAREHOUSE
-- privileges. sf_driver.py defaults to role ACCOUNTADMIN; the role
-- choice is the author's (see sf_driver.py header).
-- =====================================================================

CREATE DATABASE IF NOT EXISTS BLINDATSCALE_SF;
USE DATABASE BLINDATSCALE_SF;

CREATE SCHEMA IF NOT EXISTS BLINDATSCALE_SF.RUN;
CREATE SCHEMA IF NOT EXISTS BLINDATSCALE_SF.SMOKE;
CREATE SCHEMA IF NOT EXISTS BLINDATSCALE_SF.UTIL;

-- Warehouses. XS is the S-XENG warehouse and the S-SIZE XS leg.
-- S and M are created here but are S-SIZE-ONLY (PROGRAM_DESIGN.md §4.6);
-- no S-XENG statement runs on them. All auto-suspend after 60 s idle
-- (per-second billing, 60 s minimum per start/resume) and start
-- suspended so nothing bills until a run resumes one.
CREATE WAREHOUSE IF NOT EXISTS BAS_SF_WH_XS
  WITH WAREHOUSE_SIZE = 'XSMALL'
       WAREHOUSE_TYPE = 'STANDARD'
       AUTO_SUSPEND = 60
       AUTO_RESUME = TRUE
       INITIALLY_SUSPENDED = TRUE
       RESOURCE_CONSTRAINT = 'STANDARD_GEN_2'
  COMMENT = 'Blind at Scale S-XENG + S-SIZE XS leg (X-Small Gen2, 1.35 cr/hr)';

CREATE WAREHOUSE IF NOT EXISTS BAS_SF_WH_S
  WITH WAREHOUSE_SIZE = 'SMALL'
       WAREHOUSE_TYPE = 'STANDARD'
       AUTO_SUSPEND = 60
       AUTO_RESUME = TRUE
       INITIALLY_SUSPENDED = TRUE
       RESOURCE_CONSTRAINT = 'STANDARD_GEN_2'
  COMMENT = 'Blind at Scale S-SIZE ONLY (Small Gen2, 2.7 cr/hr)';

CREATE WAREHOUSE IF NOT EXISTS BAS_SF_WH_M
  WITH WAREHOUSE_SIZE = 'MEDIUM'
       WAREHOUSE_TYPE = 'STANDARD'
       AUTO_SUSPEND = 60
       AUTO_RESUME = TRUE
       INITIALLY_SUSPENDED = TRUE
       RESOURCE_CONSTRAINT = 'STANDARD_GEN_2'
  COMMENT = 'Blind at Scale S-SIZE ONLY (Medium Gen2, 5.4 cr/hr); first trim under the cap rule';

-- Internal stage for the uploaded parquet + counts exports.
CREATE STAGE IF NOT EXISTS BLINDATSCALE_SF.UTIL.BAS_STAGE
  COMMENT = 'Blind at Scale uploads (fact/fact2/dim parquet, counts exports)';

-- Tables. Column names match the local parquet files exactly (the COPY
-- in 02_load.sql uses MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE).
-- kN NULLs are the int32 sentinel -1, exactly as in the local files.
CREATE TABLE IF NOT EXISTS BLINDATSCALE_SF.RUN.FACT (
  kA INT, kB INT, kC INT, kN INT, s INT, v FLOAT
);
CREATE TABLE IF NOT EXISTS BLINDATSCALE_SF.RUN.FACT2 (
  kA INT, kB INT
);
CREATE TABLE IF NOT EXISTS BLINDATSCALE_SF.RUN.DIM (
  dim_key INT, payload INT
);

-- Per-key count tables, materialised by 02_load.sql after COPY.
-- One row per (column, key); used for the counts-export SHA check
-- (PROGRAM_DESIGN.md §6) and the data-level verification.
CREATE TABLE IF NOT EXISTS BLINDATSCALE_SF.RUN.FACT_COUNTS (
  COL VARCHAR, K INT, CNT NUMBER
);
CREATE TABLE IF NOT EXISTS BLINDATSCALE_SF.RUN.FACT2_COUNTS (
  COL VARCHAR, K INT, CNT NUMBER
);
