-- ============================================================
-- FINLENS DW - PHASE 5
-- 02 SECTOR ANALYSIS
-- Source: GOLD.SECTOR_PERFORMANCE
-- Grain: one sector / one date
-- ============================================================

USE DATABASE FINLENS_DW;
USE WAREHOUSE FINLENS_WH;


-- ============================================================
-- Q1. SECTOR PERFORMANCE RANKING
-- Business question:
-- Which sectors have the strongest average return?
-- ============================================================

SELECT
    SECTOR_CODE,
    SECTOR_NAME,

    AVG(AVG_DAILY_RETURN_PCT)
        AS AVG_SECTOR_RETURN_PCT,

    AVG(AVG_INTRADAY_RANGE_PCT)
        AS AVG_INTRADAY_RANGE_PCT,

    SUM(TOTAL_VOLUME)
        AS TOTAL_VOLUME

FROM GOLD.SECTOR_PERFORMANCE

WHERE AVG_DAILY_RETURN_PCT IS NOT NULL

GROUP BY
    SECTOR_CODE,
    SECTOR_NAME

ORDER BY AVG_SECTOR_RETURN_PCT DESC;


-- ============================================================
-- Q2. WORST-PERFORMING SECTORS
-- ============================================================

SELECT
    SECTOR_CODE,
    SECTOR_NAME,

    AVG(AVG_DAILY_RETURN_PCT)
        AS AVG_SECTOR_RETURN_PCT,

    SUM(TOTAL_VOLUME)
        AS TOTAL_VOLUME

FROM GOLD.SECTOR_PERFORMANCE

WHERE AVG_DAILY_RETURN_PCT IS NOT NULL

GROUP BY
    SECTOR_CODE,
    SECTOR_NAME

ORDER BY AVG_SECTOR_RETURN_PCT ASC

LIMIT 10;


-- ============================================================
-- Q3. SECTOR ACTIVITY BY VOLUME
-- Business question:
-- Which sectors have the greatest trading activity?
-- ============================================================

SELECT
    SECTOR_CODE,
    SECTOR_NAME,

    SUM(TOTAL_VOLUME)
        AS TOTAL_VOLUME,

    AVG(TOTAL_VOLUME)
        AS AVG_DAILY_VOLUME,

    MAX(TOTAL_VOLUME)
        AS MAX_DAILY_VOLUME

FROM GOLD.SECTOR_PERFORMANCE

GROUP BY
    SECTOR_CODE,
    SECTOR_NAME

ORDER BY TOTAL_VOLUME DESC

LIMIT 10;


-- ============================================================
-- Q4. SECTOR BREADTH
-- Business question:
-- How many stock observations contribute to each sector?
-- ============================================================

SELECT
    SECTOR_CODE,
    SECTOR_NAME,

    SUM(STOCK_COUNT)
        AS STOCK_OBSERVATIONS,

    MAX(STOCK_COUNT)
        AS MAX_STOCK_COUNT_IN_A_DAY,

    COUNT(*) AS OBSERVATION_DAYS

FROM GOLD.SECTOR_PERFORMANCE

GROUP BY
    SECTOR_CODE,
    SECTOR_NAME

ORDER BY STOCK_OBSERVATIONS DESC;


-- ============================================================
-- Q5. BEST SINGLE-STOCK PERFORMANCE WITHIN EACH SECTOR
-- ============================================================

SELECT
    SECTOR_CODE,
    SECTOR_NAME,

    MAX(BEST_STOCK_RETURN_PCT)
        AS BEST_STOCK_RETURN_PCT

FROM GOLD.SECTOR_PERFORMANCE

WHERE BEST_STOCK_RETURN_PCT IS NOT NULL

GROUP BY
    SECTOR_CODE,
    SECTOR_NAME

ORDER BY BEST_STOCK_RETURN_PCT DESC;


-- ============================================================
-- Q6. WORST SINGLE-STOCK PERFORMANCE WITHIN EACH SECTOR
-- ============================================================

SELECT
    SECTOR_CODE,
    SECTOR_NAME,

    MIN(WORST_STOCK_RETURN_PCT)
        AS WORST_STOCK_RETURN_PCT

FROM GOLD.SECTOR_PERFORMANCE

WHERE WORST_STOCK_RETURN_PCT IS NOT NULL

GROUP BY
    SECTOR_CODE,
    SECTOR_NAME

ORDER BY WORST_STOCK_RETURN_PCT ASC;


-- ============================================================
-- Q7. DAILY SECTOR LEADERBOARD
-- Business question:
-- Which sector led the market on each available day?
-- ============================================================

SELECT
    DATE,
    SECTOR_NAME,
    AVG_DAILY_RETURN_PCT,
    TOTAL_VOLUME,
    STOCK_COUNT

FROM GOLD.SECTOR_PERFORMANCE

WHERE AVG_DAILY_RETURN_PCT IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY DATE
    ORDER BY AVG_DAILY_RETURN_PCT DESC
) = 1

ORDER BY DATE;


-- ============================================================
-- Q8. SECTOR RETURN STABILITY
-- Business question:
-- Which sectors have relatively stable daily returns?
-- ============================================================

SELECT
    SECTOR_CODE,
    SECTOR_NAME,

    AVG(AVG_DAILY_RETURN_PCT)
        AS AVG_RETURN_PCT,

    STDDEV_SAMP(AVG_DAILY_RETURN_PCT)
        AS RETURN_STDDEV_PCT,

    COUNT(*) AS OBSERVATION_DAYS

FROM GOLD.SECTOR_PERFORMANCE

WHERE AVG_DAILY_RETURN_PCT IS NOT NULL

GROUP BY
    SECTOR_CODE,
    SECTOR_NAME

HAVING COUNT(*) >= 2

ORDER BY RETURN_STDDEV_PCT ASC;