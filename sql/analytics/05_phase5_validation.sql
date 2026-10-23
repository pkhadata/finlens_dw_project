-- ============================================================
-- FINLENS DW - PHASE 5 VALIDATION
-- ============================================================

USE DATABASE FINLENS_DW;
USE WAREHOUSE FINLENS_WH;


-- ============================================================
-- 1. GOLD ROW COUNTS
-- ============================================================

SELECT
    'STOCK_PERFORMANCE' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT
FROM GOLD.STOCK_PERFORMANCE

UNION ALL

SELECT
    'SECTOR_PERFORMANCE',
    COUNT(*)
FROM GOLD.SECTOR_PERFORMANCE

UNION ALL

SELECT
    'MARKET_PERFORMANCE',
    COUNT(*)
FROM GOLD.MARKET_PERFORMANCE

UNION ALL

SELECT
    'BOND_ANALYSIS',
    COUNT(*)
FROM GOLD.BOND_ANALYSIS;


-- ============================================================
-- 2. CHECK DUPLICATE STOCK GRAIN
-- Expected: ZERO ROWS
-- Grain = SYMBOL + DATE
-- ============================================================

SELECT
    SYMBOL,
    DATE,
    COUNT(*) AS DUPLICATE_COUNT

FROM GOLD.STOCK_PERFORMANCE

GROUP BY
    SYMBOL,
    DATE

HAVING COUNT(*) > 1;


-- ============================================================
-- 3. CHECK DUPLICATE SECTOR GRAIN
-- Expected: ZERO ROWS
-- Grain = DATE + SECTOR_CODE + SECTOR_NAME
-- ============================================================

SELECT
    DATE,
    SECTOR_CODE,
    SECTOR_NAME,
    COUNT(*) AS DUPLICATE_COUNT

FROM GOLD.SECTOR_PERFORMANCE

GROUP BY
    DATE,
    SECTOR_CODE,
    SECTOR_NAME

HAVING COUNT(*) > 1;


-- ============================================================
-- 4. CHECK DUPLICATE MARKET GRAIN
-- Expected: ZERO ROWS
-- Grain = DATE
-- ============================================================

SELECT
    DATE,
    COUNT(*) AS DUPLICATE_COUNT

FROM GOLD.MARKET_PERFORMANCE

GROUP BY DATE

HAVING COUNT(*) > 1;


-- ============================================================
-- 5. CHECK DUPLICATE BOND GRAIN
-- Expected: ZERO ROWS
-- Grain = BOND_CODE
-- ============================================================

SELECT
    BOND_CODE,
    COUNT(*) AS DUPLICATE_COUNT

FROM GOLD.BOND_ANALYSIS

GROUP BY BOND_CODE

HAVING COUNT(*) > 1;


-- ============================================================
-- 6. BASIC STOCK METRIC QUALITY
-- ============================================================

SELECT
    COUNT(*) AS TOTAL_ROWS,

    COUNT_IF(DAILY_RETURN_PCT IS NULL)
        AS NULL_RETURN,

    COUNT_IF(VOLUME IS NULL)
        AS NULL_VOLUME,

    COUNT_IF(CLOSE IS NULL)
        AS NULL_CLOSE

FROM GOLD.STOCK_PERFORMANCE;


-- ============================================================
-- 7. BASIC BOND METRIC QUALITY
-- ============================================================

SELECT
    COUNT(*) AS TOTAL_ROWS,

    COUNT_IF(COUPON_RATE_PCT IS NULL)
        AS NULL_COUPON,

    COUNT_IF(OUTSTANDING_VALUE_MVND IS NULL)
        AS NULL_OUTSTANDING,

    COUNT_IF(MATURITY_DATE IS NULL)
        AS NULL_MATURITY,

    COUNT_IF(OUTSTANDING_RATIO < 0)
        AS NEGATIVE_RATIO

FROM GOLD.BOND_ANALYSIS;


-- ============================================================
-- 8. CHECK IMPOSSIBLE STOCK METRICS
-- Expected:
-- RETURN_BELOW_MINUS_100 = 0
-- NEGATIVE_INTRADAY_RANGE = 0
-- ============================================================

SELECT
    COUNT_IF(DAILY_RETURN_PCT < -100)
        AS RETURN_BELOW_MINUS_100,

    COUNT_IF(INTRADAY_RANGE_PCT < 0)
        AS NEGATIVE_INTRADAY_RANGE

FROM GOLD.STOCK_PERFORMANCE;