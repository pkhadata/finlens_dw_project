-- ============================================================
-- FINLENS DW - PHASE 5
-- 04 BOND ANALYSIS
-- Source: GOLD.BOND_ANALYSIS
-- Grain: one bond
-- ============================================================

USE DATABASE FINLENS_DW;
USE WAREHOUSE FINLENS_WH;


-- ============================================================
-- Q1. BOND COUNT AND OUTSTANDING VALUE BY MATURITY BUCKET
-- Business question:
-- How much bond exposure exists by remaining maturity?
-- ============================================================

SELECT
    MATURITY_BUCKET,

    COUNT(*) AS BOND_COUNT,

    SUM(OUTSTANDING_VALUE_MVND)
        AS TOTAL_OUTSTANDING_MVND,

    AVG(COUPON_RATE_PCT)
        AS AVG_COUPON_RATE_PCT

FROM GOLD.BOND_ANALYSIS

GROUP BY MATURITY_BUCKET

ORDER BY MATURITY_BUCKET;


-- ============================================================
-- Q2. TOP ISSUERS BY OUTSTANDING VALUE
-- Business question:
-- Which issuers have the largest outstanding bond value?
-- ============================================================

SELECT
    ISSUER,
    MAX(ISSUER_NAME) AS ISSUER_NAME,

    COUNT(*) AS BOND_COUNT,

    SUM(OUTSTANDING_VALUE_MVND)
        AS TOTAL_OUTSTANDING_MVND,

    AVG(COUPON_RATE_PCT)
        AS AVG_COUPON_RATE_PCT

FROM GOLD.BOND_ANALYSIS

GROUP BY ISSUER

ORDER BY TOTAL_OUTSTANDING_MVND DESC

LIMIT 10;


-- ============================================================
-- Q3. HIGHEST COUPON RATE
-- Business question:
-- Which bonds have the highest coupon rates?
-- ============================================================

SELECT
    BOND_CODE,
    ISSUER_NAME,
    COUPON_RATE_PCT,
    CURRENCY,
    MATURITY_DATE,
    MATURITY_BUCKET

FROM GOLD.BOND_ANALYSIS

WHERE COUPON_RATE_PCT IS NOT NULL

ORDER BY COUPON_RATE_PCT DESC

LIMIT 10;


-- ============================================================
-- Q4. BONDS APPROACHING MATURITY
-- Business question:
-- Which bonds have the shortest remaining maturity?
-- ============================================================

SELECT
    BOND_CODE,
    ISSUER_NAME,
    MATURITY_DATE,
    DAYS_TO_MATURITY,
    YEARS_TO_MATURITY,

    OUTSTANDING_VALUE_MVND,

    COUPON_RATE_PCT

FROM GOLD.BOND_ANALYSIS

WHERE DAYS_TO_MATURITY IS NOT NULL
  AND DAYS_TO_MATURITY >= 0

ORDER BY DAYS_TO_MATURITY ASC

LIMIT 20;


-- ============================================================
-- Q5. SECURED VS UNSECURED BONDS
-- ============================================================

SELECT
    SECURED,

    COUNT(*) AS BOND_COUNT,

    SUM(OUTSTANDING_VALUE_MVND)
        AS TOTAL_OUTSTANDING_MVND,

    AVG(COUPON_RATE_PCT)
        AS AVG_COUPON_RATE_PCT

FROM GOLD.BOND_ANALYSIS

GROUP BY SECURED

ORDER BY SECURED DESC;


-- ============================================================
-- Q6. SPECIAL BOND CHARACTERISTICS
-- Business question:
-- How common are green, convertible and warrant bonds?
-- ============================================================

SELECT
    COUNT_IF(GREEN = TRUE)
        AS GREEN_BONDS,

    COUNT_IF(CONVERTIBLE = TRUE)
        AS CONVERTIBLE_BONDS,

    COUNT_IF(WITH_WARRANT = TRUE)
        AS BONDS_WITH_WARRANT,

    COUNT(*) AS TOTAL_BONDS

FROM GOLD.BOND_ANALYSIS;


-- ============================================================
-- Q7. BONDS BY BUSINESS SECTOR
-- ============================================================

SELECT
    BUSINESS_SECTOR,

    COUNT(*) AS BOND_COUNT,

    SUM(OUTSTANDING_VALUE_MVND)
        AS TOTAL_OUTSTANDING_MVND,

    AVG(COUPON_RATE_PCT)
        AS AVG_COUPON_RATE_PCT

FROM GOLD.BOND_ANALYSIS

GROUP BY BUSINESS_SECTOR

ORDER BY TOTAL_OUTSTANDING_MVND DESC;


-- ============================================================
-- Q8. OUTSTANDING RATIO ANALYSIS
-- Business question:
-- Which bonds retain the largest proportion of issued value?
-- ============================================================

SELECT
    BOND_CODE,
    ISSUER_NAME,

    ISSUED_VALUE_MVND,

    OUTSTANDING_VALUE_MVND,

    OUTSTANDING_RATIO,

    MATURITY_BUCKET

FROM GOLD.BOND_ANALYSIS

WHERE OUTSTANDING_RATIO IS NOT NULL

ORDER BY OUTSTANDING_RATIO DESC

LIMIT 20;