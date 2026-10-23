-- ============================================================
-- FINLENS DW - PHASE 5
-- 01 STOCK ANALYSIS
-- Source: GOLD.STOCK_PERFORMANCE
-- Grain: one stock / one date
-- ============================================================

USE DATABASE FINLENS_DW;
USE WAREHOUSE FINLENS_WH;


-- ============================================================
-- Q1. TOP 10 DAILY GAINERS
-- Business question:
-- Which stocks had the strongest daily gains?
-- ============================================================

SELECT
    SYMBOL,
    COMPANY_NAME,
    DATE,
    DAILY_RETURN_PCT,
    CLOSE,
    VOLUME
FROM GOLD.STOCK_PERFORMANCE
WHERE DAILY_RETURN_PCT IS NOT NULL
ORDER BY DAILY_RETURN_PCT DESC
LIMIT 10;


-- ============================================================
-- Q2. TOP 10 DAILY LOSERS
-- ============================================================

SELECT
    SYMBOL,
    COMPANY_NAME,
    DATE,
    DAILY_RETURN_PCT,
    CLOSE,
    VOLUME
FROM GOLD.STOCK_PERFORMANCE
WHERE DAILY_RETURN_PCT IS NOT NULL
ORDER BY DAILY_RETURN_PCT ASC
LIMIT 10;


-- ============================================================
-- Q3. STOCK PERFORMANCE SUMMARY
-- Business question:
-- Which stocks performed best across the available observations?
-- ============================================================

SELECT
    SYMBOL,
    MAX(COMPANY_NAME) AS COMPANY_NAME,
    COUNT(*) AS OBSERVATION_DAYS,

    AVG(DAILY_RETURN_PCT)
        AS AVG_DAILY_RETURN_PCT,

    SUM(VOLUME)
        AS TOTAL_VOLUME,

    AVG(VOLATILITY_5D_PCT)
        AS AVG_VOLATILITY_5D_PCT,

    MAX(CUMULATIVE_RETURN_PCT)
        AS MAX_CUMULATIVE_RETURN_PCT

FROM GOLD.STOCK_PERFORMANCE

GROUP BY SYMBOL

ORDER BY AVG_DAILY_RETURN_PCT DESC;


-- ============================================================
-- Q4. MOST VOLATILE STOCKS
-- Business question:
-- Which stocks show the largest short-term price variability?
-- ============================================================

SELECT
    SYMBOL,
    MAX(COMPANY_NAME) AS COMPANY_NAME,

    AVG(VOLATILITY_5D_PCT)
        AS AVG_VOLATILITY_5D_PCT,

    MAX(VOLATILITY_5D_PCT)
        AS MAX_VOLATILITY_5D_PCT,

    COUNT(*) AS OBSERVATION_DAYS

FROM GOLD.STOCK_PERFORMANCE

WHERE VOLATILITY_5D_PCT IS NOT NULL

GROUP BY SYMBOL

ORDER BY AVG_VOLATILITY_5D_PCT DESC

LIMIT 10;


-- ============================================================
-- Q5. HIGHEST TRADING VOLUME
-- Business question:
-- Which stocks generated the highest observed trading volume?
-- ============================================================

SELECT
    SYMBOL,
    MAX(COMPANY_NAME) AS COMPANY_NAME,

    SUM(VOLUME)
        AS TOTAL_VOLUME,

    AVG(VOLUME)
        AS AVG_DAILY_VOLUME,

    MAX(VOLUME)
        AS MAX_DAILY_VOLUME

FROM GOLD.STOCK_PERFORMANCE

GROUP BY SYMBOL

ORDER BY TOTAL_VOLUME DESC

LIMIT 10;


-- ============================================================
-- Q6. BEST CUMULATIVE PERFORMANCE
-- Business question:
-- Which stocks gained the most from their first available close?
-- ============================================================

SELECT
    SYMBOL,
    MAX(COMPANY_NAME) AS COMPANY_NAME,

    MAX(CUMULATIVE_RETURN_PCT)
        AS CUMULATIVE_RETURN_PCT

FROM GOLD.STOCK_PERFORMANCE

WHERE CUMULATIVE_RETURN_PCT IS NOT NULL

GROUP BY SYMBOL

ORDER BY CUMULATIVE_RETURN_PCT DESC

LIMIT 10;


-- ============================================================
-- Q7. DAILY STOCK MARKET BREADTH
-- Business question:
-- How many stocks rose, fell, or were unchanged each day?
-- ============================================================

SELECT
    DATE,

    COUNT(*) AS STOCK_OBSERVATIONS,

    COUNT_IF(DAILY_RETURN_PCT > 0)
        AS RISING_STOCKS,

    COUNT_IF(DAILY_RETURN_PCT < 0)
        AS FALLING_STOCKS,

    COUNT_IF(DAILY_RETURN_PCT = 0)
        AS UNCHANGED_STOCKS,

    AVG(DAILY_RETURN_PCT)
        AS AVG_DAILY_RETURN_PCT

FROM GOLD.STOCK_PERFORMANCE

GROUP BY DATE

ORDER BY DATE;


-- ============================================================
-- Q8. LARGEST INTRADAY RANGES
-- Business question:
-- Which observations had the widest high-low price range?
-- ============================================================

SELECT
    SYMBOL,
    COMPANY_NAME,
    DATE,
    INTRADAY_RANGE_PCT,
    CLOSE

FROM GOLD.STOCK_PERFORMANCE

WHERE INTRADAY_RANGE_PCT IS NOT NULL

ORDER BY INTRADAY_RANGE_PCT DESC

LIMIT 10;