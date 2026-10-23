-- ============================================================
-- FINLENS DW - PHASE 5
-- 03 MARKET ANALYSIS
-- Source: GOLD.MARKET_PERFORMANCE
-- Grain: one date
-- ============================================================

USE DATABASE FINLENS_DW;
USE WAREHOUSE FINLENS_WH;


-- ============================================================
-- Q1. MARKET TREND
-- ============================================================

SELECT
    DATE,
    AVG_INDEX_CLOSE,
    AVG_DAILY_RETURN_PCT,
    MARKET_CUMULATIVE_RETURN_PCT,
    TOTAL_VOLUME

FROM GOLD.MARKET_PERFORMANCE

ORDER BY DATE;


-- ============================================================
-- Q2. BEST MARKET DAYS
-- Business question:
-- Which days had the strongest market return?
-- ============================================================

SELECT
    DATE,
    AVG_DAILY_RETURN_PCT,
    MARKET_CUMULATIVE_RETURN_PCT,
    TOTAL_VOLUME

FROM GOLD.MARKET_PERFORMANCE

WHERE AVG_DAILY_RETURN_PCT IS NOT NULL

ORDER BY AVG_DAILY_RETURN_PCT DESC

LIMIT 10;


-- ============================================================
-- Q3. WORST MARKET DAYS
-- ============================================================

SELECT
    DATE,
    AVG_DAILY_RETURN_PCT,
    MARKET_CUMULATIVE_RETURN_PCT,
    TOTAL_VOLUME

FROM GOLD.MARKET_PERFORMANCE

WHERE AVG_DAILY_RETURN_PCT IS NOT NULL

ORDER BY AVG_DAILY_RETURN_PCT ASC

LIMIT 10;


-- ============================================================
-- Q4. MARKET BREADTH / INDEX COVERAGE
-- Business question:
-- How many index observations were available on each day?
-- ============================================================

SELECT
    DATE,
    INDEX_COUNT,
    AVG_DAILY_RETURN_PCT,
    TOTAL_VOLUME

FROM GOLD.MARKET_PERFORMANCE

ORDER BY DATE;


-- ============================================================
-- Q5. MARKET VOLUME RANKING
-- Business question:
-- Which days had the highest aggregate index volume?
-- ============================================================

SELECT
    DATE,
    TOTAL_VOLUME,
    AVG_DAILY_RETURN_PCT,
    AVG_INDEX_CLOSE

FROM GOLD.MARKET_PERFORMANCE

ORDER BY TOTAL_VOLUME DESC

LIMIT 10;


-- ============================================================
-- Q6. MARKET RETURN STATISTICS
-- ============================================================

SELECT
    COUNT(*) AS OBSERVATION_DAYS,

    AVG(AVG_DAILY_RETURN_PCT)
        AS AVG_MARKET_RETURN_PCT,

    STDDEV_SAMP(AVG_DAILY_RETURN_PCT)
        AS MARKET_RETURN_STDDEV_PCT,

    MAX(AVG_DAILY_RETURN_PCT)
        AS BEST_DAY_RETURN_PCT,

    MIN(AVG_DAILY_RETURN_PCT)
        AS WORST_DAY_RETURN_PCT

FROM GOLD.MARKET_PERFORMANCE

WHERE AVG_DAILY_RETURN_PCT IS NOT NULL;


-- ============================================================
-- Q7. POSITIVE VS NEGATIVE MARKET DAYS
-- Business question:
-- Was the available period dominated by rising or falling days?
-- ============================================================

SELECT
    COUNT_IF(AVG_DAILY_RETURN_PCT > 0)
        AS POSITIVE_DAYS,

    COUNT_IF(AVG_DAILY_RETURN_PCT < 0)
        AS NEGATIVE_DAYS,

    COUNT_IF(AVG_DAILY_RETURN_PCT = 0)
        AS FLAT_DAYS,

    COUNT(*) AS TOTAL_DAYS

FROM GOLD.MARKET_PERFORMANCE

WHERE AVG_DAILY_RETURN_PCT IS NOT NULL;


-- ============================================================
-- Q8. HIGHEST-RETURN MARKET DAY
-- ============================================================

SELECT
    DATE,
    AVG_DAILY_RETURN_PCT,
    TOTAL_VOLUME,
    AVG_INDEX_CLOSE,
    BEST_INDEX_RETURN_PCT,
    WORST_INDEX_RETURN_PCT

FROM GOLD.MARKET_PERFORMANCE

WHERE AVG_DAILY_RETURN_PCT IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
    ORDER BY AVG_DAILY_RETURN_PCT DESC
) = 1;