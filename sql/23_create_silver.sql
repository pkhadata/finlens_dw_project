-- FINLENS DW - PHASE 3
-- Reference SQL / documentation version.
-- The executable implementation is build_silver.py.

USE WAREHOUSE FINLENS_WH;
USE DATABASE FINLENS_DW;

CREATE SCHEMA IF NOT EXISTS SILVER;

-- The actual table definitions and transformations are maintained
-- in build_silver.py so the Phase 3 build is reproducible.
--
-- Output:
--   SILVER.DIM_DATE
--   SILVER.DIM_SYMBOL
--   SILVER.DIM_SECTOR
--   SILVER.FACT_STOCK_DAILY
--   SILVER.FACT_INDEX_DAILY
--   SILVER.FACT_CORPORATE_BOND
