import os
import sys
import traceback
from datetime import datetime

import snowflake.connector
from dotenv import load_dotenv


load_dotenv()


DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FINLENS_DW")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "FINLENS_WH")
ROLE = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")


def env(name, required=True):
    value = os.getenv(name)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def execute(cur, sql, label):
    print(f"[RUN] {label}")
    cur.execute(sql)
    print(f"[OK ] {label}")


def connect_snowflake():
    return snowflake.connector.connect(
        account=env("SNOWFLAKE_ACCOUNT"),
        user=env("SNOWFLAKE_USER"),
        password=env("SNOWFLAKE_PASSWORD"),
        warehouse=WAREHOUSE,
        database=DATABASE,
        role=ROLE,
        client_session_keep_alive=False,
    )


def build_dimensions(cur):
    execute(cur, """
        CREATE OR REPLACE TABLE SILVER.DIM_DATE (
            FULL_DATE DATE NOT NULL,
            YEAR NUMBER(4,0),
            QUARTER NUMBER(1,0),
            MONTH NUMBER(2,0),
            MONTH_NAME VARCHAR,
            WEEK NUMBER(2,0),
            DAY_OF_MONTH NUMBER(2,0),
            DAY_OF_WEEK NUMBER(1,0),
            DAY_NAME VARCHAR,
            IS_WEEKEND BOOLEAN,
            CONSTRAINT PK_DIM_DATE PRIMARY KEY (FULL_DATE)
        )
    """, "Create DIM_DATE")

    execute(cur, """
        INSERT INTO SILVER.DIM_DATE
        WITH bounds AS (
            SELECT
                LEAST(
                    (SELECT MIN(DATE) FROM BRONZE.STOCK_OHLCV_DAILY),
                    (SELECT MIN(DATE) FROM BRONZE.INDEX_OHLCV_DAILY)
                ) AS MIN_DATE,
                GREATEST(
                    (SELECT MAX(DATE) FROM BRONZE.STOCK_OHLCV_DAILY),
                    (SELECT MAX(DATE) FROM BRONZE.INDEX_OHLCV_DAILY)
                ) AS MAX_DATE
        ),
        dates AS (
            SELECT DATEADD(DAY, SEQ4(), MIN_DATE)::DATE AS FULL_DATE
            FROM bounds,
                 TABLE(GENERATOR(ROWCOUNT => 10000))
            WHERE DATEADD(DAY, SEQ4(), MIN_DATE)::DATE <= MAX_DATE
        )
        SELECT
            FULL_DATE,
            YEAR(FULL_DATE),
            QUARTER(FULL_DATE),
            MONTH(FULL_DATE),
            MONTHNAME(FULL_DATE),
            WEEKOFYEAR(FULL_DATE),
            DAY(FULL_DATE),
            DAYOFWEEKISO(FULL_DATE),
            DAYNAME(FULL_DATE),
            DAYOFWEEKISO(FULL_DATE) IN (6, 7)
        FROM dates
        ORDER BY FULL_DATE
    """, "Populate DIM_DATE")

    execute(cur, """
        CREATE OR REPLACE TABLE SILVER.DIM_SYMBOL (
            SYMBOL VARCHAR NOT NULL,
            EXCHANGE VARCHAR,
            KIND VARCHAR,
            COMPANY_TYPE VARCHAR,
            ICB_LEVEL4 VARCHAR,
            ICB_NAME4 VARCHAR,
            ICB_LEVEL2 VARCHAR,
            ICB_NAME2 VARCHAR,
            NAME VARCHAR,
            SHORT_NAME VARCHAR,
            CONSTRAINT PK_DIM_SYMBOL PRIMARY KEY (SYMBOL)
        )
    """, "Create DIM_SYMBOL")

    execute(cur, """
        INSERT INTO SILVER.DIM_SYMBOL
        SELECT
            TRIM(SYMBOL),
            NULLIF(TRIM(EXCHANGE), ''),
            NULLIF(TRIM(KIND), ''),
            NULLIF(TRIM(COMPANY_TYPE), ''),
            NULLIF(TRIM(ICB_LEVEL4), ''),
            NULLIF(TRIM(ICB_NAME4), ''),
            NULLIF(TRIM(ICB_LEVEL2), ''),
            NULLIF(TRIM(ICB_NAME2), ''),
            NULLIF(TRIM(NAME), ''),
            NULLIF(TRIM(SHORT_NAME), '')
        FROM BRONZE.SYMBOLS
        WHERE NULLIF(TRIM(SYMBOL), '') IS NOT NULL
    """, "Populate DIM_SYMBOL")

    execute(cur, """
        CREATE OR REPLACE TABLE SILVER.DIM_SECTOR (
            ICB VARCHAR NOT NULL,
            LEVEL NUMBER,
            NAME_VI VARCHAR,
            PARENT_ICB VARCHAR,
            CONSTRAINT PK_DIM_SECTOR PRIMARY KEY (ICB)
        )
    """, "Create DIM_SECTOR")

    execute(cur, """
        INSERT INTO SILVER.DIM_SECTOR
        SELECT
            NULLIF(TRIM(ICB), ''),
            LEVEL,
            NULLIF(TRIM(NAME_VI), ''),
            NULLIF(TRIM(PARENT_ICB), '')
        FROM BRONZE.SECTORS
        WHERE NULLIF(TRIM(ICB), '') IS NOT NULL
    """, "Populate DIM_SECTOR")


def build_facts(cur):
    execute(cur, """
        CREATE OR REPLACE TABLE SILVER.FACT_STOCK_DAILY (
            SYMBOL VARCHAR NOT NULL,
            DATE DATE NOT NULL,
            OPEN NUMBER(38,10),
            HIGH NUMBER(38,10),
            LOW NUMBER(38,10),
            CLOSE NUMBER(38,10),
            VOLUME NUMBER(38,10),
            DAILY_RETURN_PCT NUMBER(18,6),
            INTRADAY_RANGE_PCT NUMBER(18,6),
            CONSTRAINT PK_FACT_STOCK_DAILY PRIMARY KEY (SYMBOL, DATE)
        )
    """, "Create FACT_STOCK_DAILY")

    execute(cur, """
        INSERT INTO SILVER.FACT_STOCK_DAILY (
            SYMBOL,
            DATE,
            OPEN,
            HIGH,
            LOW,
            CLOSE,
            VOLUME,
            DAILY_RETURN_PCT,
            INTRADAY_RANGE_PCT
        )
        WITH base AS (
            SELECT
                TRIM(SYMBOL) AS SYMBOL,
                DATE,
                OPEN,
                HIGH,
                LOW,
                CLOSE,
                VOLUME,
                LAG(CLOSE) OVER (
                    PARTITION BY TRIM(SYMBOL)
                    ORDER BY DATE
                ) AS PREV_CLOSE
            FROM BRONZE.STOCK_OHLCV_DAILY
            WHERE NULLIF(TRIM(SYMBOL), '') IS NOT NULL
        )
        SELECT
            SYMBOL,
            DATE,
            OPEN,
            HIGH,
            LOW,
            CLOSE,
            VOLUME,
            CASE
                WHEN PREV_CLOSE IS NOT NULL AND PREV_CLOSE <> 0
                THEN ((CLOSE - PREV_CLOSE) / PREV_CLOSE) * 100
                ELSE NULL
            END AS DAILY_RETURN_PCT,
            CASE
                WHEN CLOSE IS NOT NULL AND CLOSE <> 0
                THEN ((HIGH - LOW) / CLOSE) * 100
                ELSE NULL
            END AS INTRADAY_RANGE_PCT
        FROM base
    """, "Populate FACT_STOCK_DAILY")

    execute(cur, """
        CREATE OR REPLACE TABLE SILVER.FACT_INDEX_DAILY (
            SYMBOL VARCHAR NOT NULL,
            DATE DATE NOT NULL,
            OPEN NUMBER(38,10),
            HIGH NUMBER(38,10),
            LOW NUMBER(38,10),
            CLOSE NUMBER(38,10),
            VOLUME NUMBER(38,10),
            DAILY_RETURN_PCT NUMBER(18,6),
            CONSTRAINT PK_FACT_INDEX_DAILY PRIMARY KEY (SYMBOL, DATE)
        )
    """, "Create FACT_INDEX_DAILY")

    execute(cur, """
        INSERT INTO SILVER.FACT_INDEX_DAILY (
            SYMBOL,
            DATE,
            OPEN,
            HIGH,
            LOW,
            CLOSE,
            VOLUME,
            DAILY_RETURN_PCT
        )
        WITH base AS (
            SELECT
                TRIM(SYMBOL) AS SYMBOL,
                DATE,
                OPEN,
                HIGH,
                LOW,
                CLOSE,
                VOLUME,
                LAG(CLOSE) OVER (
                    PARTITION BY TRIM(SYMBOL)
                    ORDER BY DATE
                ) AS PREV_CLOSE
            FROM BRONZE.INDEX_OHLCV_DAILY
            WHERE NULLIF(TRIM(SYMBOL), '') IS NOT NULL
        )
        SELECT
            SYMBOL,
            DATE,
            OPEN,
            HIGH,
            LOW,
            CLOSE,
            VOLUME,
            CASE
                WHEN PREV_CLOSE IS NOT NULL AND PREV_CLOSE <> 0
                THEN ((CLOSE - PREV_CLOSE) / PREV_CLOSE) * 100
                ELSE NULL
            END AS DAILY_RETURN_PCT
        FROM base
    """, "Populate FACT_INDEX_DAILY")

    execute(cur, """
        CREATE OR REPLACE TABLE SILVER.FACT_CORPORATE_BOND (
            BOND_CODE VARCHAR NOT NULL,
            SYMBOL VARCHAR,
            ISSUER VARCHAR,
            ISSUER_NAME VARCHAR,
            TRADING_CODE VARCHAR,
            ISIN VARCHAR,
            INTEREST_PERIOD VARCHAR,
            CURRENCY VARCHAR,
            BUSINESS_SECTOR VARCHAR,
            BOND_STATUS VARCHAR,
            REGISTRATION_STATUS VARCHAR,
            INTEREST_TYPE VARCHAR,
            RATE_TYPE VARCHAR,
            PRINCIPAL_PAYMENT_METHOD VARCHAR,
            INTEREST_PAYMENT_METHOD VARCHAR,
            COUPON_RATE_PCT NUMBER(18,8),
            ISSUE_DATE DATE,
            MATURITY_DATE DATE,
            PAR_VALUE_MVND NUMBER(38,10),
            PAR_VALUE_USD NUMBER(38,10),
            ISSUED_VALUE_MVND NUMBER(38,10),
            ISSUED_VALUE_USD NUMBER(38,10),
            OUTSTANDING_VALUE_MVND NUMBER(38,10),
            OUTSTANDING_VALUE_USD NUMBER(38,10),
            CONVERTIBLE BOOLEAN,
            SECURED BOOLEAN,
            GREEN BOOLEAN,
            WITH_WARRANT BOOLEAN,
            DAYS_TO_MATURITY NUMBER(18,0),
            YEARS_TO_MATURITY NUMBER(18,6),
            OUTSTANDING_RATIO NUMBER(18,6),
            CONSTRAINT PK_FACT_CORPORATE_BOND PRIMARY KEY (BOND_CODE)
        )
    """, "Create FACT_CORPORATE_BOND")

    execute(cur, """
        INSERT INTO SILVER.FACT_CORPORATE_BOND (
            BOND_CODE,
            SYMBOL,
            ISSUER,
            ISSUER_NAME,
            TRADING_CODE,
            ISIN,
            INTEREST_PERIOD,
            CURRENCY,
            BUSINESS_SECTOR,
            BOND_STATUS,
            REGISTRATION_STATUS,
            INTEREST_TYPE,
            RATE_TYPE,
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
            CONVERTIBLE,
            SECURED,
            GREEN,
            WITH_WARRANT,
            DAYS_TO_MATURITY,
            YEARS_TO_MATURITY,
            OUTSTANDING_RATIO
        )
        SELECT
            NULLIF(TRIM(BOND_CODE), ''),
            NULLIF(TRIM(SYMBOL), ''),
            NULLIF(TRIM(ISSUER), ''),
            NULLIF(TRIM(ISSUER_NAME), ''),
            NULLIF(TRIM(TRADING_CODE), ''),
            NULLIF(TRIM(ISIN), ''),
            NULLIF(TRIM(INTEREST_PERIOD), ''),
            NULLIF(TRIM(CURRENCY), ''),
            NULLIF(TRIM(BUSINESS_SECTOR), ''),
            NULLIF(TRIM(BOND_STATUS), ''),
            NULLIF(TRIM(REGISTRATION_STATUS), ''),
            NULLIF(TRIM(INTEREST_TYPE), ''),
            NULLIF(TRIM(RATE_TYPE), ''),
            NULLIF(TRIM(PRINCIPAL_PAYMENT_METHOD), ''),
            NULLIF(TRIM(INTEREST_PAYMENT_METHOD), ''),
            COUPON_RATE_PCT,
            ISSUE_DATE,
            MATURITY_DATE,
            PAR_VALUE_MVND,
            PAR_VALUE_USD,
            ISSUED_VALUE_MVND,
            ISSUED_VALUE_USD,
            OUTSTANDING_VALUE_MVND,
            OUTSTANDING_VALUE_USD,
            CONVERTIBLE,
            SECURED,
            GREEN,
            WITH_WARRANT,
            CASE
                WHEN MATURITY_DATE IS NOT NULL
                THEN DATEDIFF(DAY, CURRENT_DATE(), MATURITY_DATE)
                ELSE NULL
            END AS DAYS_TO_MATURITY,
            CASE
                WHEN MATURITY_DATE IS NOT NULL
                THEN DATEDIFF(DAY, CURRENT_DATE(), MATURITY_DATE) / 365.25
                ELSE NULL
            END AS YEARS_TO_MATURITY,
            CASE
                WHEN ISSUED_VALUE_MVND IS NOT NULL
                     AND ISSUED_VALUE_MVND <> 0
                THEN OUTSTANDING_VALUE_MVND / ISSUED_VALUE_MVND
                ELSE NULL
            END AS OUTSTANDING_RATIO
        FROM BRONZE.CORPORATE_BONDS
        WHERE NULLIF(TRIM(BOND_CODE), '') IS NOT NULL
    """, "Populate FACT_CORPORATE_BOND")


def validate(cur):
    expected = {
        "DIM_DATE": None,
        "DIM_SYMBOL": 1650,
        "DIM_SECTOR": 125,
        "FACT_STOCK_DAILY": 81,
        "FACT_INDEX_DAILY": 996,
        "FACT_CORPORATE_BOND": 2045,
    }

    print("\n" + "=" * 70)
    print("SILVER VALIDATION")
    print("=" * 70)

    for table, expected_count in expected.items():
        cur.execute(f"SELECT COUNT(*) FROM SILVER.{table}")
        count = cur.fetchone()[0]

        if expected_count is None:
            print(f"{table:<25} {count:>8} rows [PASS]")
        elif count == expected_count:
            print(f"{table:<25} {count:>8} rows [PASS]")
        else:
            raise RuntimeError(
                f"{table}: expected {expected_count} rows, got {count}"
            )

    checks = [
        ("DIM_SYMBOL", "SYMBOL"),
        ("DIM_SECTOR", "ICB"),
        ("FACT_STOCK_DAILY", "SYMBOL || '|' || DATE::VARCHAR"),
        ("FACT_INDEX_DAILY", "SYMBOL || '|' || DATE::VARCHAR"),
        ("FACT_CORPORATE_BOND", "BOND_CODE"),
    ]

    for table, key_expr in checks:
        cur.execute(f"""
            SELECT COUNT(*), COUNT(DISTINCT {key_expr})
            FROM SILVER.{table}
        """)
        total, distinct_count = cur.fetchone()

        if total != distinct_count:
            raise RuntimeError(
                f"{table}: duplicate grain detected "
                f"(rows={total}, distinct={distinct_count})"
            )

        print(f"{table:<25} duplicate grain [PASS]")

    print("\nSilver validation completed successfully.")


def main():
    print("=" * 70)
    print("FINLENS DW - PHASE 3 SILVER BUILD")
    print("=" * 70)
    print("\nConnecting to Snowflake...")

    conn = None
    cur = None

    try:
        conn = connect_snowflake()
        cur = conn.cursor()
        print("Snowflake connection: OK")

        execute(cur, f"USE WAREHOUSE {WAREHOUSE}", "Use warehouse")
        execute(cur, f"USE DATABASE {DATABASE}", "Use database")
        execute(cur, "CREATE SCHEMA IF NOT EXISTS SILVER", "Ensure SILVER schema")

        build_dimensions(cur)
        build_facts(cur)

        conn.commit()
        validate(cur)

        print("\n" + "=" * 70)
        print("PHASE 3 COMPLETED SUCCESSFULLY")
        print("=" * 70)

    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass

        print("\n" + "=" * 70)
        print("PHASE 3 FAILED")
        print("=" * 70)
        print(type(exc).__name__, ":", exc)
        traceback.print_exc()
        sys.exit(1)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
