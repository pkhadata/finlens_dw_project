import os
import uuid
from datetime import datetime

import snowflake.connector
from dotenv import load_dotenv


# ============================================================
# FINLENS DW - PHASE 4 GOLD BUILD
# ============================================================

load_dotenv()


SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "FINLENS_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "FINLENS_DW"),
    "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
}


def connect_snowflake():
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG["account"],
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        role=SNOWFLAKE_CONFIG["role"],
        client_session_keep_alive=False,
    )
    print("Snowflake connection: OK")
    return conn


def execute(conn, label, sql):
    print(f"[RUN] {label}")

    cur = conn.cursor()
    try:
        cur.execute(sql)
        print(f"[OK ] {label}")
    finally:
        cur.close()


def query_one(conn, sql):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()


def validate_count(conn, table_name, expected):
    sql = f"SELECT COUNT(*) FROM GOLD.{table_name}"
    actual = query_one(conn, sql)

    status = "PASS" if actual == expected else "FAIL"

    print(
        f"{table_name:<25} "
        f"{actual:>8} rows "
        f"[{status}]"
    )

    if status == "FAIL":
        raise RuntimeError(
            f"{table_name}: expected {expected}, got {actual}"
        )


def validate_no_duplicate(conn, table_name, grain_columns):
    grain = ", ".join(grain_columns)

    sql = f"""
        SELECT COUNT(*)
        FROM (
            SELECT {grain}
            FROM GOLD.{table_name}
            GROUP BY {grain}
            HAVING COUNT(*) > 1
        )
    """

    duplicate_groups = query_one(conn, sql)

    status = "PASS" if duplicate_groups == 0 else "FAIL"

    print(
        f"{table_name:<25} "
        f"duplicate grain "
        f"[{status}]"
    )

    if status == "FAIL":
        raise RuntimeError(
            f"{table_name}: duplicate grain detected"
        )


def main():

    print("=" * 70)
    print("FINLENS DW - PHASE 4 GOLD BUILD")
    print("=" * 70)

    conn = connect_snowflake()

    try:

        # --------------------------------------------------------
        # SESSION
        # --------------------------------------------------------

        execute(
            conn,
            "Use warehouse",
            "USE WAREHOUSE FINLENS_WH"
        )

        execute(
            conn,
            "Use database",
            "USE DATABASE FINLENS_DW"
        )

        execute(
            conn,
            "Ensure GOLD schema",
            """
            CREATE SCHEMA IF NOT EXISTS FINLENS_DW.GOLD
            """
        )

        # --------------------------------------------------------
        # 1. STOCK PERFORMANCE
        # --------------------------------------------------------

        execute(
            conn,
            "Create GOLD.STOCK_PERFORMANCE",
            """
            CREATE OR REPLACE TABLE GOLD.STOCK_PERFORMANCE (
                SYMBOL STRING,
                DATE DATE,

                EXCHANGE STRING,
                COMPANY_NAME STRING,
                SHORT_NAME STRING,

                CLOSE FLOAT,
                VOLUME FLOAT,

                DAILY_RETURN_PCT FLOAT,
                INTRADAY_RANGE_PCT FLOAT,

                CUMULATIVE_RETURN_PCT FLOAT,

                AVG_VOLUME_5D FLOAT,
                VOLATILITY_5D_PCT FLOAT
            )
            """
        )

        execute(
            conn,
            "Populate GOLD.STOCK_PERFORMANCE",
            """
            INSERT INTO GOLD.STOCK_PERFORMANCE
            (
                SYMBOL,
                DATE,
                EXCHANGE,
                COMPANY_NAME,
                SHORT_NAME,
                CLOSE,
                VOLUME,
                DAILY_RETURN_PCT,
                INTRADAY_RANGE_PCT,
                CUMULATIVE_RETURN_PCT,
                AVG_VOLUME_5D,
                VOLATILITY_5D_PCT
            )

            WITH base AS (
                SELECT
                    f.SYMBOL,
                    f.DATE,

                    d.EXCHANGE,
                    d.NAME AS COMPANY_NAME,
                    d.SHORT_NAME,

                    f.CLOSE,
                    f.VOLUME,

                    f.DAILY_RETURN_PCT,
                    f.INTRADAY_RANGE_PCT,

                    FIRST_VALUE(f.CLOSE)
                        OVER (
                            PARTITION BY f.SYMBOL
                            ORDER BY f.DATE
                            ROWS BETWEEN UNBOUNDED PRECEDING
                            AND UNBOUNDED FOLLOWING
                        ) AS FIRST_CLOSE

                FROM SILVER.FACT_STOCK_DAILY f

                LEFT JOIN SILVER.DIM_SYMBOL d
                    ON f.SYMBOL = d.SYMBOL
            ),

            metrics AS (
                SELECT
                    *,

                    CASE
                        WHEN FIRST_CLOSE IS NOT NULL
                             AND FIRST_CLOSE <> 0
                        THEN
                            ((CLOSE / FIRST_CLOSE) - 1) * 100
                        ELSE NULL
                    END AS CUMULATIVE_RETURN_PCT,

                    AVG(VOLUME)
                        OVER (
                            PARTITION BY SYMBOL
                            ORDER BY DATE
                            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                        ) AS AVG_VOLUME_5D,

                    STDDEV_SAMP(DAILY_RETURN_PCT)
                        OVER (
                            PARTITION BY SYMBOL
                            ORDER BY DATE
                            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                        ) AS VOLATILITY_5D_PCT

                FROM base
            )

            SELECT
                SYMBOL,
                DATE,
                EXCHANGE,
                COMPANY_NAME,
                SHORT_NAME,
                CLOSE,
                VOLUME,
                DAILY_RETURN_PCT,
                INTRADAY_RANGE_PCT,
                CUMULATIVE_RETURN_PCT,
                AVG_VOLUME_5D,
                VOLATILITY_5D_PCT

            FROM metrics
            """
        )

        # --------------------------------------------------------
        # 2. SECTOR PERFORMANCE
        # --------------------------------------------------------

        execute(
            conn,
            "Create GOLD.SECTOR_PERFORMANCE",
            """
            CREATE OR REPLACE TABLE GOLD.SECTOR_PERFORMANCE (
                DATE DATE,

                SECTOR_CODE STRING,
                SECTOR_NAME STRING,

                STOCK_COUNT NUMBER,
                TOTAL_VOLUME FLOAT,

                AVG_DAILY_RETURN_PCT FLOAT,
                AVG_INTRADAY_RANGE_PCT FLOAT,

                BEST_STOCK_RETURN_PCT FLOAT,
                WORST_STOCK_RETURN_PCT FLOAT
            )
            """
        )

        execute(
            conn,
            "Populate GOLD.SECTOR_PERFORMANCE",
            """
            INSERT INTO GOLD.SECTOR_PERFORMANCE
            (
                DATE,
                SECTOR_CODE,
                SECTOR_NAME,
                STOCK_COUNT,
                TOTAL_VOLUME,
                AVG_DAILY_RETURN_PCT,
                AVG_INTRADAY_RANGE_PCT,
                BEST_STOCK_RETURN_PCT,
                WORST_STOCK_RETURN_PCT
            )

            SELECT
                f.DATE,

                d.ICB_LEVEL2 AS SECTOR_CODE,
                d.ICB_NAME2 AS SECTOR_NAME,

                COUNT(DISTINCT f.SYMBOL) AS STOCK_COUNT,

                SUM(f.VOLUME) AS TOTAL_VOLUME,

                AVG(f.DAILY_RETURN_PCT)
                    AS AVG_DAILY_RETURN_PCT,

                AVG(f.INTRADAY_RANGE_PCT)
                    AS AVG_INTRADAY_RANGE_PCT,

                MAX(f.DAILY_RETURN_PCT)
                    AS BEST_STOCK_RETURN_PCT,

                MIN(f.DAILY_RETURN_PCT)
                    AS WORST_STOCK_RETURN_PCT

            FROM SILVER.FACT_STOCK_DAILY f

            INNER JOIN SILVER.DIM_SYMBOL d
                ON f.SYMBOL = d.SYMBOL

            GROUP BY
                f.DATE,
                d.ICB_LEVEL2,
                d.ICB_NAME2
            """
        )

        # --------------------------------------------------------
        # 3. MARKET PERFORMANCE
        # --------------------------------------------------------

        execute(
            conn,
            "Create GOLD.MARKET_PERFORMANCE",
            """
            CREATE OR REPLACE TABLE GOLD.MARKET_PERFORMANCE (
                DATE DATE,

                INDEX_COUNT NUMBER,

                AVG_INDEX_CLOSE FLOAT,
                TOTAL_VOLUME FLOAT,

                AVG_DAILY_RETURN_PCT FLOAT,
                BEST_INDEX_RETURN_PCT FLOAT,
                WORST_INDEX_RETURN_PCT FLOAT,

                MARKET_CUMULATIVE_RETURN_PCT FLOAT
            )
            """
        )

        execute(
            conn,
            "Populate GOLD.MARKET_PERFORMANCE",
            """
            INSERT INTO GOLD.MARKET_PERFORMANCE
            (
                DATE,
                INDEX_COUNT,
                AVG_INDEX_CLOSE,
                TOTAL_VOLUME,
                AVG_DAILY_RETURN_PCT,
                BEST_INDEX_RETURN_PCT,
                WORST_INDEX_RETURN_PCT,
                MARKET_CUMULATIVE_RETURN_PCT
            )

            WITH daily AS (
                SELECT
                    DATE,

                    COUNT(DISTINCT SYMBOL)
                        AS INDEX_COUNT,

                    AVG(CLOSE)
                        AS AVG_INDEX_CLOSE,

                    SUM(VOLUME)
                        AS TOTAL_VOLUME,

                    AVG(DAILY_RETURN_PCT)
                        AS AVG_DAILY_RETURN_PCT,

                    MAX(DAILY_RETURN_PCT)
                        AS BEST_INDEX_RETURN_PCT,

                    MIN(DAILY_RETURN_PCT)
                        AS WORST_INDEX_RETURN_PCT

                FROM SILVER.FACT_INDEX_DAILY

                GROUP BY DATE
            ),

            cumulative AS (
                SELECT
                    *,

                    SUM(
                        COALESCE(AVG_DAILY_RETURN_PCT, 0)
                    ) OVER (
                        ORDER BY DATE
                        ROWS BETWEEN UNBOUNDED PRECEDING
                        AND CURRENT ROW
                    ) AS MARKET_CUMULATIVE_RETURN_PCT

                FROM daily
            )

            SELECT
                DATE,
                INDEX_COUNT,
                AVG_INDEX_CLOSE,
                TOTAL_VOLUME,
                AVG_DAILY_RETURN_PCT,
                BEST_INDEX_RETURN_PCT,
                WORST_INDEX_RETURN_PCT,
                MARKET_CUMULATIVE_RETURN_PCT

            FROM cumulative
            """
        )

        # --------------------------------------------------------
        # 4. BOND ANALYSIS
        # --------------------------------------------------------

        execute(
            conn,
            "Create GOLD.BOND_ANALYSIS",
            """
            CREATE OR REPLACE TABLE GOLD.BOND_ANALYSIS (
                BOND_CODE STRING,
                SYMBOL STRING,

                ISSUER STRING,
                ISSUER_NAME STRING,
                TRADING_CODE STRING,
                ISIN STRING,

                BUSINESS_SECTOR STRING,
                BOND_STATUS STRING,
                REGISTRATION_STATUS STRING,

                CURRENCY STRING,
                INTEREST_TYPE STRING,
                RATE_TYPE STRING,
                INTEREST_PERIOD STRING,

                PRINCIPAL_PAYMENT_METHOD STRING,
                INTEREST_PAYMENT_METHOD STRING,

                COUPON_RATE_PCT FLOAT,

                ISSUE_DATE DATE,
                MATURITY_DATE DATE,

                PAR_VALUE_MVND FLOAT,
                PAR_VALUE_USD FLOAT,

                ISSUED_VALUE_MVND FLOAT,
                ISSUED_VALUE_USD FLOAT,

                OUTSTANDING_VALUE_MVND FLOAT,
                OUTSTANDING_VALUE_USD FLOAT,

                DAYS_TO_MATURITY NUMBER,
                YEARS_TO_MATURITY FLOAT,

                OUTSTANDING_RATIO FLOAT,

                CONVERTIBLE BOOLEAN,
                SECURED BOOLEAN,
                GREEN BOOLEAN,
                WITH_WARRANT BOOLEAN,

                MATURITY_BUCKET STRING
            )
            """
        )

        execute(
            conn,
            "Populate GOLD.BOND_ANALYSIS",
            """
            INSERT INTO GOLD.BOND_ANALYSIS
            (
                BOND_CODE,
                SYMBOL,

                ISSUER,
                ISSUER_NAME,
                TRADING_CODE,
                ISIN,

                BUSINESS_SECTOR,
                BOND_STATUS,
                REGISTRATION_STATUS,

                CURRENCY,
                INTEREST_TYPE,
                RATE_TYPE,
                INTEREST_PERIOD,

                PRINCIPAL_PAYMENT_METHOD,
                INTEREST_PAYMENT_METHOD,

                COUPON_RATE_PCT,

                ISSUE_DATE,
                MATURITY_DATE,

                PAR_VALUE_MVND,
                PAR_VALUE_USD,

                ISSUED_VALUE_MVND,
                ISSUED_VALUE_USD,

                OUTSTANDING_VALUE_MVND,
                OUTSTANDING_VALUE_USD,

                DAYS_TO_MATURITY,
                YEARS_TO_MATURITY,

                OUTSTANDING_RATIO,

                CONVERTIBLE,
                SECURED,
                GREEN,
                WITH_WARRANT,

                MATURITY_BUCKET
            )

            SELECT
                BOND_CODE,
                SYMBOL,

                ISSUER,
                ISSUER_NAME,
                TRADING_CODE,
                ISIN,

                BUSINESS_SECTOR,
                BOND_STATUS,
                REGISTRATION_STATUS,

                CURRENCY,
                INTEREST_TYPE,
                RATE_TYPE,
                INTEREST_PERIOD,

                PRINCIPAL_PAYMENT_METHOD,
                INTEREST_PAYMENT_METHOD,

                COUPON_RATE_PCT,

                ISSUE_DATE,
                MATURITY_DATE,

                PAR_VALUE_MVND,
                PAR_VALUE_USD,

                ISSUED_VALUE_MVND,
                ISSUED_VALUE_USD,

                OUTSTANDING_VALUE_MVND,
                OUTSTANDING_VALUE_USD,

                DAYS_TO_MATURITY,
                YEARS_TO_MATURITY,

                OUTSTANDING_RATIO,

                CONVERTIBLE,
                SECURED,
                GREEN,
                WITH_WARRANT,

                CASE
                    WHEN YEARS_TO_MATURITY IS NULL
                        THEN 'UNKNOWN'

                    WHEN YEARS_TO_MATURITY < 1
                        THEN '< 1 YEAR'

                    WHEN YEARS_TO_MATURITY < 3
                        THEN '1 - 3 YEARS'

                    WHEN YEARS_TO_MATURITY < 5
                        THEN '3 - 5 YEARS'

                    WHEN YEARS_TO_MATURITY < 10
                        THEN '5 - 10 YEARS'

                    ELSE '>= 10 YEARS'
                END AS MATURITY_BUCKET

            FROM SILVER.FACT_CORPORATE_BOND
            """
        )
        # --------------------------------------------------------
        # VALIDATION
        # --------------------------------------------------------

        print()
        print("GOLD VALIDATION")

        expected_stock = query_one(
            conn,
            """
            SELECT COUNT(*)
            FROM SILVER.FACT_STOCK_DAILY
            """
        )

        expected_bond = query_one(
            conn,
            """
            SELECT COUNT(*)
            FROM SILVER.FACT_CORPORATE_BOND
            """
        )

        expected_market = query_one(
            conn,
            """
            SELECT COUNT(DISTINCT DATE)
            FROM SILVER.FACT_INDEX_DAILY
            """
        )

        expected_sector = query_one(
            conn,
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    f.DATE,
                    d.ICB_LEVEL2,
                    d.ICB_NAME2
                FROM SILVER.FACT_STOCK_DAILY f
                INNER JOIN SILVER.DIM_SYMBOL d
                    ON f.SYMBOL = d.SYMBOL
                GROUP BY
                    f.DATE,
                    d.ICB_LEVEL2,
                    d.ICB_NAME2
            )
            """
        )

        validate_count(
            conn,
            "STOCK_PERFORMANCE",
            expected_stock
        )

        validate_count(
            conn,
            "SECTOR_PERFORMANCE",
            expected_sector
        )

        validate_count(
            conn,
            "MARKET_PERFORMANCE",
            expected_market
        )

        validate_count(
            conn,
            "BOND_ANALYSIS",
            expected_bond
        )

        validate_no_duplicate(
            conn,
            "STOCK_PERFORMANCE",
            ["SYMBOL", "DATE"]
        )

        validate_no_duplicate(
            conn,
            "SECTOR_PERFORMANCE",
            [
                "DATE",
                "SECTOR_CODE",
                "SECTOR_NAME"
            ]
        )

        validate_no_duplicate(
            conn,
            "MARKET_PERFORMANCE",
            ["DATE"]
        )

        validate_no_duplicate(
            conn,
            "BOND_ANALYSIS",
            ["BOND_CODE"]
        )

        # --------------------------------------------------------
        # SAMPLE STATISTICS
        # --------------------------------------------------------

        print()
        print("GOLD SUMMARY")

        stock_count = query_one(
            conn,
            """
            SELECT COUNT(DISTINCT SYMBOL)
            FROM GOLD.STOCK_PERFORMANCE
            """
        )

        sector_count = query_one(
            conn,
            """
            SELECT COUNT(DISTINCT SECTOR_CODE)
            FROM GOLD.SECTOR_PERFORMANCE
            """
        )

        bond_count = query_one(
            conn,
            """
            SELECT COUNT(DISTINCT BOND_CODE)
            FROM GOLD.BOND_ANALYSIS
            """
        )

        print(f"Stocks analyzed       : {stock_count}")
        print(f"Sectors analyzed      : {sector_count}")
        print(f"Bonds analyzed        : {bond_count}")

        print()
        print("=" * 70)
        print("PHASE 4 COMPLETED SUCCESSFULLY")
        print("=" * 70)

    except Exception:
        print()
        print("PHASE 4 FAILED")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()