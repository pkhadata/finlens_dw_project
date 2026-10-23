from __future__ import annotations

import os
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import snowflake.connector
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
PIPELINE_NAME = "FINLENS_FULL_CYCLE"
TOTAL_STEPS = 5

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Natural keys in the PostgreSQL source and matching Bronze tables.
SOURCE_TABLES = [
    ("symbols", ("symbol",), ("symbol",)),
    ("sectors", ("icb",), ("icb",)),
    ("stock_ohlcv_daily", ("symbol", "date"), ("symbol",)),
    ("index_ohlcv_daily", ("symbol", "date"), ("symbol",)),
    ("corporate_bonds", ("bond_code",), ("bond_code",)),
]


# ============================================================
# GENERAL HELPERS
# ============================================================


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    remaining = seconds % 60
    return f"{minutes}m {remaining:.1f}s"


def safe_identifier(value: str, label: str) -> str:
    """Allow only simple SQL identifiers sourced from configuration."""
    if not IDENTIFIER_RE.fullmatch(value or ""):
        raise ValueError(
            f"Invalid {label}: {value!r}. Use letters, digits, and underscores only."
        )
    return value


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_database_name() -> str:
    return safe_identifier(
        os.getenv("SNOWFLAKE_DATABASE", "FINLENS_DW").strip(),
        "SNOWFLAKE_DATABASE",
    ).upper()


def qualified_table(schema: str, table: str) -> str:
    database = get_database_name()
    schema = safe_identifier(schema, "schema").upper()
    table = safe_identifier(table, "table").upper()
    return f"{database}.{schema}.{table}"


def connect_snowflake():
    return snowflake.connector.connect(
        account=required_env("SNOWFLAKE_ACCOUNT"),
        user=required_env("SNOWFLAKE_USER"),
        password=required_env("SNOWFLAKE_PASSWORD"),
        warehouse=safe_identifier(
            os.getenv("SNOWFLAKE_WAREHOUSE", "FINLENS_WH").strip(),
            "SNOWFLAKE_WAREHOUSE",
        ),
        database=get_database_name(),
        role=safe_identifier(
            os.getenv("SNOWFLAKE_ROLE", "SYSADMIN").strip(),
            "SNOWFLAKE_ROLE",
        ),
        client_session_keep_alive=False,
    )


def connect_source_engine():
    schema = safe_identifier(
        os.getenv("SOURCE_SCHEMA", "finlens").strip(), "SOURCE_SCHEMA"
    )
    port_text = os.getenv("SOURCE_PORT", "5432").strip()
    try:
        port = int(port_text)
    except ValueError as exc:
        raise RuntimeError("SOURCE_PORT must be an integer.") from exc

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=required_env("SOURCE_USER"),
        password=required_env("SOURCE_PASSWORD"),
        host=required_env("SOURCE_HOST"),
        port=port,
        database=required_env("SOURCE_DBNAME"),
    )
    return create_engine(url, pool_pre_ping=True), schema


def scalar(cur, query: str, params=None):
    if params is None:
        cur.execute(query)
    else:
        cur.execute(query, params)
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("Validation query returned no rows.")
    return row[0]


def table_count(cur, schema: str, table: str) -> int:
    return int(scalar(cur, f"SELECT COUNT(*) FROM {qualified_table(schema, table)}"))


def report_check(
    failures: list[str],
    label: str,
    passed: bool,
    detail: str,
) -> None:
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {label}: {detail}")
    if not passed:
        failures.append(f"{label}: {detail}")


def source_identifier(name: str) -> str:
    return '"' + safe_identifier(name, "source identifier") + '"'


def source_table_name(schema: str, table: str) -> str:
    return f'{source_identifier(schema)}.{source_identifier(table)}'


def duplicate_group_count(
    cur,
    full_table_name: str,
    key_columns: tuple[str, ...],
) -> int:
    columns = ", ".join(key_columns)
    query = f"""
        SELECT COUNT(*)
        FROM (
            SELECT {columns}
            FROM {full_table_name}
            GROUP BY {columns}
            HAVING COUNT(*) > 1
        ) AS duplicate_groups
    """
    return int(scalar(cur, query))


def null_or_blank_key_count(
    cur,
    full_table_name: str,
    key_columns: tuple[str, ...],
    text_columns: tuple[str, ...],
    *,
    postgres: bool = False,
) -> int:
    predicates: list[str] = []
    text_set = set(text_columns)

    for column in key_columns:
        if postgres:
            quoted = source_identifier(column)
            predicate = f"{quoted} IS NULL"
            if column in text_set:
                predicate += f" OR BTRIM(CAST({quoted} AS TEXT)) = ''"
        else:
            quoted = safe_identifier(column, "key column").upper()
            predicate = f"{quoted} IS NULL"
            if column in text_set:
                predicate += f" OR TRIM({quoted}) = ''"
        predicates.append(f"({predicate})")

    query = f"SELECT COUNT(*) FROM {full_table_name} WHERE " + " OR ".join(predicates)
    if postgres:
        return int(cur.execute(text(query)).scalar_one())
    return int(scalar(cur, query))


# ============================================================
# STEP 2/5 — BRONZE VALIDATION
# ============================================================


def validate_bronze() -> None:
    """Compare PostgreSQL source to Bronze and verify key/metadata quality."""
    print("Comparing PostgreSQL source tables with Snowflake Bronze...")
    print("Natural-key duplicates, blank keys, and ingestion metadata are checked too.\n")

    engine, source_schema = connect_source_engine()
    snowflake_conn = None
    failures: list[str] = []

    try:
        snowflake_conn = connect_snowflake()
        with engine.connect() as source_conn:
            with snowflake_conn.cursor() as cur:
                for table, key_columns, text_key_columns in SOURCE_TABLES:
                    source_fqn = source_table_name(source_schema, table)
                    bronze_fqn = qualified_table("BRONZE", table)

                    source_rows = int(
                        source_conn.execute(
                            text(f"SELECT COUNT(*) FROM {source_fqn}")
                        ).scalar_one()
                    )
                    bronze_rows = table_count(cur, "BRONZE", table)

                    pg_keys = ", ".join(source_identifier(col) for col in key_columns)
                    source_duplicate_sql = f"""
                        SELECT COUNT(*)
                        FROM (
                            SELECT {pg_keys}
                            FROM {source_fqn}
                            GROUP BY {pg_keys}
                            HAVING COUNT(*) > 1
                        ) AS duplicate_groups
                    """
                    source_duplicates = int(
                        source_conn.execute(text(source_duplicate_sql)).scalar_one()
                    )
                    bronze_duplicates = duplicate_group_count(
                        cur,
                        bronze_fqn,
                        tuple(col.upper() for col in key_columns),
                    )

                    source_bad_keys = null_or_blank_key_count(
                        source_conn,
                        source_fqn,
                        key_columns,
                        text_key_columns,
                        postgres=True,
                    )
                    bronze_bad_keys = null_or_blank_key_count(
                        cur,
                        bronze_fqn,
                        tuple(col.upper() for col in key_columns),
                        tuple(col.upper() for col in text_key_columns),
                    )

                    metadata_bad_rows = int(
                        scalar(
                            cur,
                            f"""
                            SELECT COALESCE(
                                COUNT_IF(
                                    _BATCH_ID IS NULL
                                    OR _LOADED_AT IS NULL
                                    OR _SOURCE_SYSTEM IS NULL
                                ),
                                0
                            )
                            FROM {bronze_fqn}
                            """,
                        )
                    )

                    table_passed = (
                        source_rows > 0
                        and source_rows == bronze_rows
                        and source_duplicates == 0
                        and bronze_duplicates == 0
                        and source_bad_keys == 0
                        and bronze_bad_keys == 0
                        and metadata_bad_rows == 0
                    )

                    detail = (
                        f"source={source_rows:,}, bronze={bronze_rows:,}, "
                        f"duplicates(source/bronze)={source_duplicates}/{bronze_duplicates}, "
                        f"blank-or-null-keys(source/bronze)={source_bad_keys}/{bronze_bad_keys}, "
                        f"rows-with-null-metadata={metadata_bad_rows}"
                    )
                    report_check(failures, f"BRONZE.{table.upper()}", table_passed, detail)

    finally:
        engine.dispose()
        if snowflake_conn is not None:
            snowflake_conn.close()

    if failures:
        message = "\n".join(f"  - {item}" for item in failures)
        raise RuntimeError("Bronze validation failed:\n" + message)

    print("\nBronze validation passed for all five source tables.")


# ============================================================
# STEP 5/5 — END-TO-END VALIDATION HELPERS
# ============================================================


def table_columns(cur, schema: str, table: str) -> dict[str, str]:
    database = get_database_name()
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """
    cur.execute(query, (schema.upper(), table.upper()))
    rows = cur.fetchall()
    columns = {str(name).upper(): str(dtype).upper() for name, dtype in rows}
    if not columns:
        raise RuntimeError(
            f"Could not find columns for {database}.{schema.upper()}.{table.upper()}."
        )
    return columns


def choose_column(
    columns: dict[str, str],
    candidates: tuple[str, ...],
    description: str,
) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    raise RuntimeError(
        f"Could not identify {description}. Available columns: {', '.join(columns)}"
    )


def check_grain(
    cur,
    failures: list[str],
    schema: str,
    table: str,
    key_columns: tuple[str, ...],
    columns: dict[str, str] | None = None,
) -> None:
    fqn = qualified_table(schema, table)
    table_columns_map = columns or table_columns(cur, schema, table)
    missing = [col for col in key_columns if col.upper() not in table_columns_map]
    if missing:
        raise RuntimeError(
            f"{schema}.{table} is missing grain column(s): {', '.join(missing)}. "
            f"Available columns: {', '.join(table_columns_map)}"
        )

    text_types = {"TEXT", "VARCHAR", "CHAR", "CHARACTER", "STRING"}
    text_keys = tuple(
        col.upper()
        for col in key_columns
        if table_columns_map[col.upper()] in text_types
    )
    normalized_keys = tuple(col.upper() for col in key_columns)

    bad_keys = null_or_blank_key_count(
        cur,
        fqn,
        normalized_keys,
        text_keys,
    )
    duplicate_groups = duplicate_group_count(cur, fqn, normalized_keys)
    passed = bad_keys == 0 and duplicate_groups == 0
    report_check(
        failures,
        f"{schema}.{table} grain",
        passed,
        f"null/blank keys={bad_keys}, duplicate key groups={duplicate_groups}",
    )


def check_count_match(
    cur,
    failures: list[str],
    label: str,
    left_query: str,
    right_query: str,
) -> None:
    left_count = int(scalar(cur, left_query))
    right_count = int(scalar(cur, right_query))
    report_check(
        failures,
        label,
        left_count == right_count,
        f"left={left_count:,}, right={right_count:,}",
    )


def sector_expression_for_silver(symbol_columns: dict[str, str], gold_sector_column: str) -> str:
    """Build a sector grouping expression from the Silver symbol attributes."""
    column = gold_sector_column.upper()

    if column in symbol_columns and column in {
        "ICB_NAME4", "ICB_NAME2", "ICB_LEVEL4", "ICB_LEVEL2"
    }:
        return f"COALESCE(NULLIF(TRIM(S.{column}), ''), 'UNKNOWN')"

    # Gold commonly exposes a human-readable sector label instead of the ICB
    # source column name. This fallback uses the same hierarchy available in Silver.
    if column in {
        "SECTOR_NAME", "SECTOR", "SECTOR_LABEL", "BUSINESS_SECTOR", "SECTOR_CODE"
    }:
        if column == "SECTOR_CODE":
            return (
                "COALESCE(NULLIF(TRIM(S.ICB_LEVEL4), ''), "
                "NULLIF(TRIM(S.ICB_LEVEL2), ''), 'UNKNOWN')"
            )
        return (
            "COALESCE(NULLIF(TRIM(S.ICB_NAME4), ''), "
            "NULLIF(TRIM(S.ICB_NAME2), ''), "
            "NULLIF(TRIM(S.ICB_LEVEL4), ''), "
            "NULLIF(TRIM(S.ICB_LEVEL2), ''), 'UNKNOWN')"
        )

    if column in {"ICB", "ICB_CODE", "SECTOR_ICB"}:
        return (
            "COALESCE(NULLIF(TRIM(S.ICB_LEVEL4), ''), "
            "NULLIF(TRIM(S.ICB_LEVEL2), ''), 'UNKNOWN')"
        )

    raise RuntimeError(
        f"Cannot derive expected Silver sector grouping for Gold column {gold_sector_column!r}."
    )


def validate_e2e() -> None:
    """Validate count lineage, grains, date coverage, and computed metrics."""
    failures: list[str] = []
    conn = connect_snowflake()

    try:
        with conn.cursor() as cur:
            print("\n1) Layer-to-layer row-count reconciliation")
            count_relationships = [
                (
                    "BRONZE.SYMBOLS → SILVER.DIM_SYMBOL",
                    f"SELECT COUNT(*) FROM {qualified_table('BRONZE', 'SYMBOLS')}",
                    f"SELECT COUNT(*) FROM {qualified_table('SILVER', 'DIM_SYMBOL')}",
                ),
                (
                    "BRONZE.SECTORS → SILVER.DIM_SECTOR",
                    f"SELECT COUNT(*) FROM {qualified_table('BRONZE', 'SECTORS')}",
                    f"SELECT COUNT(*) FROM {qualified_table('SILVER', 'DIM_SECTOR')}",
                ),
                (
                    "BRONZE.STOCK_OHLCV_DAILY → SILVER.FACT_STOCK_DAILY",
                    f"SELECT COUNT(*) FROM {qualified_table('BRONZE', 'STOCK_OHLCV_DAILY')}",
                    f"SELECT COUNT(*) FROM {qualified_table('SILVER', 'FACT_STOCK_DAILY')}",
                ),
                (
                    "BRONZE.INDEX_OHLCV_DAILY → SILVER.FACT_INDEX_DAILY",
                    f"SELECT COUNT(*) FROM {qualified_table('BRONZE', 'INDEX_OHLCV_DAILY')}",
                    f"SELECT COUNT(*) FROM {qualified_table('SILVER', 'FACT_INDEX_DAILY')}",
                ),
                (
                    "BRONZE.CORPORATE_BONDS → SILVER.FACT_CORPORATE_BOND",
                    f"SELECT COUNT(*) FROM {qualified_table('BRONZE', 'CORPORATE_BONDS')}",
                    f"SELECT COUNT(*) FROM {qualified_table('SILVER', 'FACT_CORPORATE_BOND')}",
                ),
                (
                    "SILVER.FACT_STOCK_DAILY → GOLD.STOCK_PERFORMANCE",
                    f"SELECT COUNT(*) FROM {qualified_table('SILVER', 'FACT_STOCK_DAILY')}",
                    f"SELECT COUNT(*) FROM {qualified_table('GOLD', 'STOCK_PERFORMANCE')}",
                ),
                (
                    "SILVER.FACT_CORPORATE_BOND → GOLD.BOND_ANALYSIS",
                    f"SELECT COUNT(*) FROM {qualified_table('SILVER', 'FACT_CORPORATE_BOND')}",
                    f"SELECT COUNT(*) FROM {qualified_table('GOLD', 'BOND_ANALYSIS')}",
                ),
            ]
            for label, left_query, right_query in count_relationships:
                check_count_match(cur, failures, label, left_query, right_query)

            # Gold market output is one row per market observation date, while
            # Silver index facts contain one row per index/date pair.
            market_rows = table_count(cur, "GOLD", "MARKET_PERFORMANCE")
            market_dates = int(
                scalar(
                    cur,
                    f"""
                    SELECT COUNT(DISTINCT DATE)
                    FROM {qualified_table('SILVER', 'FACT_INDEX_DAILY')}
                    """,
                )
            )
            report_check(
                failures,
                "SILVER index dates → GOLD.MARKET_PERFORMANCE",
                market_rows == market_dates,
                f"distinct Silver dates={market_dates:,}, Gold rows={market_rows:,}",
            )

            # Gold sector performance is aggregated to sector/date grain.
            gold_sector_columns = table_columns(cur, "GOLD", "SECTOR_PERFORMANCE")
            silver_symbol_columns = table_columns(cur, "SILVER", "DIM_SYMBOL")
            gold_sector_key = choose_column(
                gold_sector_columns,
                (
                    "SECTOR_NAME", "SECTOR", "SECTOR_LABEL", "ICB_NAME4",
                    "ICB_NAME2", "ICB_LEVEL4", "ICB_LEVEL2", "ICB",
                    "ICB_CODE", "SECTOR_CODE", "SECTOR_ICB", "BUSINESS_SECTOR",
                ),
                "sector key in GOLD.SECTOR_PERFORMANCE",
            )
            gold_sector_date = choose_column(
                gold_sector_columns,
                ("DATE", "TRADE_DATE", "FULL_DATE"),
                "date key in GOLD.SECTOR_PERFORMANCE",
            )
            sector_expr = sector_expression_for_silver(
                silver_symbol_columns, gold_sector_key
            )
            expected_sector_groups = int(
                scalar(
                    cur,
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT F.DATE, {sector_expr} AS SECTOR_KEY
                        FROM {qualified_table('SILVER', 'FACT_STOCK_DAILY')} F
                        JOIN {qualified_table('SILVER', 'DIM_SYMBOL')} S
                          ON F.SYMBOL = S.SYMBOL
                        GROUP BY F.DATE, {sector_expr}
                    ) AS expected_sector_groups
                    """,
                )
            )
            gold_sector_rows = table_count(cur, "GOLD", "SECTOR_PERFORMANCE")
            report_check(
                failures,
                "Silver stock facts → GOLD.SECTOR_PERFORMANCE",
                gold_sector_rows == expected_sector_groups,
                f"expected sector/date groups={expected_sector_groups:,}, "
                f"Gold rows={gold_sector_rows:,}",
            )

            print("\n2) Required tables are populated")
            required_tables = [
                ("SILVER", "DIM_DATE"),
                ("SILVER", "DIM_SYMBOL"),
                ("SILVER", "DIM_SECTOR"),
                ("SILVER", "FACT_STOCK_DAILY"),
                ("SILVER", "FACT_INDEX_DAILY"),
                ("SILVER", "FACT_CORPORATE_BOND"),
                ("GOLD", "STOCK_PERFORMANCE"),
                ("GOLD", "SECTOR_PERFORMANCE"),
                ("GOLD", "MARKET_PERFORMANCE"),
                ("GOLD", "BOND_ANALYSIS"),
            ]
            counts: dict[tuple[str, str], int] = {}
            for schema, table in required_tables:
                rows = table_count(cur, schema, table)
                counts[(schema, table)] = rows
                report_check(
                    failures,
                    f"{schema}.{table} populated",
                    rows > 0,
                    f"rows={rows:,}",
                )

            print("\n3) Silver and Gold key/grain validation")
            grain_specs: list[tuple[str, str, tuple[str, ...], dict[str, str] | None]] = [
                ("SILVER", "DIM_DATE", ("FULL_DATE",), None),
                ("SILVER", "DIM_SYMBOL", ("SYMBOL",), None),
                ("SILVER", "DIM_SECTOR", ("ICB",), None),
                ("SILVER", "FACT_STOCK_DAILY", ("SYMBOL", "DATE"), None),
                ("SILVER", "FACT_INDEX_DAILY", ("SYMBOL", "DATE"), None),
                ("SILVER", "FACT_CORPORATE_BOND", ("BOND_CODE",), None),
                ("GOLD", "BOND_ANALYSIS", ("BOND_CODE",), None),
            ]
            for schema, table, keys, columns in grain_specs:
                check_grain(cur, failures, schema, table, keys, columns)

            stock_gold_columns = table_columns(cur, "GOLD", "STOCK_PERFORMANCE")
            stock_gold_symbol = choose_column(
                stock_gold_columns, ("SYMBOL", "TICKER", "STOCK_SYMBOL"),
                "symbol key in GOLD.STOCK_PERFORMANCE",
            )
            stock_gold_date = choose_column(
                stock_gold_columns, ("DATE", "TRADE_DATE", "FULL_DATE"),
                "date key in GOLD.STOCK_PERFORMANCE",
            )
            check_grain(
                cur, failures, "GOLD", "STOCK_PERFORMANCE",
                (stock_gold_symbol, stock_gold_date), stock_gold_columns,
            )

            market_columns = table_columns(cur, "GOLD", "MARKET_PERFORMANCE")
            market_date_col = choose_column(
                market_columns,
                ("DATE", "TRADE_DATE", "FULL_DATE"),
                "date key in GOLD.MARKET_PERFORMANCE",
            )
            check_grain(
                cur,
                failures,
                "GOLD",
                "MARKET_PERFORMANCE",
                (market_date_col,),
                market_columns,
            )
            check_grain(
                cur,
                failures,
                "GOLD",
                "SECTOR_PERFORMANCE",
                (gold_sector_key, gold_sector_date),
                gold_sector_columns,
            )

            print("\n4) Date dimension coverage")
            dim_date_stats = cur.execute(
                f"""
                SELECT COUNT(*), MIN(FULL_DATE), MAX(FULL_DATE)
                FROM {qualified_table('SILVER', 'DIM_DATE')}
                """
            ).fetchone()
            date_rows, min_date, max_date = dim_date_stats
            expected_bounds = cur.execute(
                f"""
                SELECT
                    LEAST(
                        (SELECT MIN(DATE) FROM {qualified_table('SILVER', 'FACT_STOCK_DAILY')}),
                        (SELECT MIN(DATE) FROM {qualified_table('SILVER', 'FACT_INDEX_DAILY')})
                    ),
                    GREATEST(
                        (SELECT MAX(DATE) FROM {qualified_table('SILVER', 'FACT_STOCK_DAILY')}),
                        (SELECT MAX(DATE) FROM {qualified_table('SILVER', 'FACT_INDEX_DAILY')})
                    )
                """
            ).fetchone()
            expected_min, expected_max = expected_bounds
            contiguous = (
                date_rows > 0
                and min_date is not None
                and max_date is not None
                and int((max_date - min_date).days) + 1 == int(date_rows)
            )
            bounds_match = min_date == expected_min and max_date == expected_max
            report_check(
                failures,
                "SILVER.DIM_DATE continuity",
                contiguous,
                f"rows={date_rows}, range={min_date}..{max_date}",
            )
            report_check(
                failures,
                "SILVER.DIM_DATE source bounds",
                bounds_match,
                f"actual={min_date}..{max_date}, expected={expected_min}..{expected_max}",
            )

            print("\n5) Business metric quality")
            stock_bad = int(
                scalar(
                    cur,
                    f"""
                    SELECT COUNT(*)
                    FROM {qualified_table('SILVER', 'FACT_STOCK_DAILY')}
                    WHERE (HIGH IS NOT NULL AND LOW IS NOT NULL AND HIGH < LOW)
                       OR (OPEN IS NOT NULL AND LOW IS NOT NULL AND OPEN < LOW)
                       OR (OPEN IS NOT NULL AND HIGH IS NOT NULL AND OPEN > HIGH)
                       OR (CLOSE IS NOT NULL AND LOW IS NOT NULL AND CLOSE < LOW)
                       OR (CLOSE IS NOT NULL AND HIGH IS NOT NULL AND CLOSE > HIGH)
                       OR (VOLUME IS NOT NULL AND VOLUME < 0)
                       OR (DAILY_RETURN_PCT IS NOT NULL AND DAILY_RETURN_PCT < -100)
                       OR (INTRADAY_RANGE_PCT IS NOT NULL AND INTRADAY_RANGE_PCT < 0)
                       OR (
                           INTRADAY_RANGE_PCT IS NOT NULL
                           AND (HIGH IS NULL OR LOW IS NULL OR CLOSE IS NULL OR CLOSE = 0)
                       )
                    """,
                )
            )
            report_check(
                failures,
                "SILVER.FACT_STOCK_DAILY metrics",
                stock_bad == 0,
                f"invalid OHLC/volume/return/range rows={stock_bad}",
            )

            index_bad = int(
                scalar(
                    cur,
                    f"""
                    SELECT COUNT(*)
                    FROM {qualified_table('SILVER', 'FACT_INDEX_DAILY')}
                    WHERE (HIGH IS NOT NULL AND LOW IS NOT NULL AND HIGH < LOW)
                       OR (OPEN IS NOT NULL AND LOW IS NOT NULL AND OPEN < LOW)
                       OR (OPEN IS NOT NULL AND HIGH IS NOT NULL AND OPEN > HIGH)
                       OR (CLOSE IS NOT NULL AND LOW IS NOT NULL AND CLOSE < LOW)
                       OR (CLOSE IS NOT NULL AND HIGH IS NOT NULL AND CLOSE > HIGH)
                       OR (VOLUME IS NOT NULL AND VOLUME < 0)
                       OR (DAILY_RETURN_PCT IS NOT NULL AND DAILY_RETURN_PCT < -100)
                    """,
                )
            )
            report_check(
                failures,
                "SILVER.FACT_INDEX_DAILY metrics",
                index_bad == 0,
                f"invalid OHLC/volume/return rows={index_bad}",
            )

            bond_bad = int(
                scalar(
                    cur,
                    f"""
                    SELECT COUNT(*)
                    FROM {qualified_table('SILVER', 'FACT_CORPORATE_BOND')}
                    WHERE (
                        MATURITY_DATE IS NOT NULL
                        AND (
                            DAYS_TO_MATURITY IS NULL
                            OR YEARS_TO_MATURITY IS NULL
                            OR DAYS_TO_MATURITY <> DATEDIFF(DAY, CURRENT_DATE(), MATURITY_DATE)
                            OR ABS(
                                YEARS_TO_MATURITY
                                - DATEDIFF(DAY, CURRENT_DATE(), MATURITY_DATE) / 365.25
                            ) > 0.000001
                        )
                    )
                    OR (
                        MATURITY_DATE IS NULL
                        AND (DAYS_TO_MATURITY IS NOT NULL OR YEARS_TO_MATURITY IS NOT NULL)
                    )
                    OR (
                        OUTSTANDING_RATIO IS NOT NULL
                        AND (
                            ISSUED_VALUE_MVND IS NULL
                            OR ISSUED_VALUE_MVND = 0
                            OR OUTSTANDING_VALUE_MVND IS NULL
                            OR OUTSTANDING_RATIO < 0
                        )
                    )
                    OR (
                        ISSUED_VALUE_MVND IS NOT NULL
                        AND ISSUED_VALUE_MVND <> 0
                        AND OUTSTANDING_VALUE_MVND IS NOT NULL
                        AND (
                            OUTSTANDING_RATIO IS NULL
                            OR ABS(
                                OUTSTANDING_RATIO
                                - OUTSTANDING_VALUE_MVND / ISSUED_VALUE_MVND
                            ) > 0.000001
                        )
                    )
                    """,
                )
            )
            report_check(
                failures,
                "SILVER.FACT_CORPORATE_BOND metrics",
                bond_bad == 0,
                f"invalid maturity/outstanding-ratio rows={bond_bad}",
            )

    finally:
        conn.close()

    if failures:
        message = "\n".join(f"  - {item}" for item in failures)
        raise RuntimeError("End-to-end validation failed:\n" + message)

    print("\nAll end-to-end data quality checks passed.")


# ============================================================
# PIPELINE STEPS
# ============================================================


def run_script(script: str) -> None:
    script_path = SCRIPTS_DIR / script
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    command = [sys.executable, str(script_path)]
    print("Command:")
    print(" ".join(f'"{item}"' if " " in item else item for item in command))
    print()

    result = subprocess.run(command, cwd=PROJECT_ROOT)
    if result.returncode != 0:
        raise RuntimeError(f"Script {script} exited with code {result.returncode}.")


def run_callable(func: Callable[[], None]) -> None:
    func()


# ============================================================
# MAIN PIPELINE
# ============================================================


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    load_dotenv(SCRIPTS_DIR / ".env")

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    pipeline_started = time.perf_counter()
    started_at = utc_now()

    steps: list[tuple[str, str | None, Callable[[], None] | None]] = [
        ("BRONZE INGESTION", "ingest_to_snowflake.py", None),
        ("BRONZE VALIDATION", None, validate_bronze),
        ("SILVER BUILD", "build_silver.py", None),
        ("GOLD BUILD", "build_gold.py", None),
        ("END-TO-END VALIDATION", None, validate_e2e),
    ]

    results: list[dict[str, str | float]] = []
    failure_message: str | None = None
    interrupted = False

    print("\n" + "=" * 70)
    print("FINLENS DW - END-TO-END DATA PIPELINE")
    print("=" * 70)
    print(f"Pipeline : {PIPELINE_NAME}")
    print(f"Run ID   : {run_id}")
    print(f"Started  : {started_at.isoformat()}")

    try:
        for index, (name, script, validator) in enumerate(steps, start=1):
            print("\n" + "=" * 70)
            print(f"[{index}/{TOTAL_STEPS}] {name}")
            print("=" * 70 + "\n")
            step_started = time.perf_counter()

            try:
                if script is not None:
                    run_script(script)
                elif validator is not None:
                    run_callable(validator)
                else:
                    raise RuntimeError(f"Step has no action configured: {name}")
            except KeyboardInterrupt:
                elapsed = time.perf_counter() - step_started
                results.append({
                    "name": name,
                    "status": "INTERRUPTED",
                    "duration": elapsed,
                })
                print(f"\n[INTERRUPTED] {name} (duration={format_duration(elapsed)})")
                raise
            except Exception as exc:
                elapsed = time.perf_counter() - step_started
                results.append({
                    "name": name,
                    "status": "FAIL",
                    "duration": elapsed,
                })
                print(f"\n[FAIL] {name} (duration={format_duration(elapsed)})")
                raise RuntimeError(f"{name}: {type(exc).__name__}: {exc}") from exc
            else:
                elapsed = time.perf_counter() - step_started
                results.append({
                    "name": name,
                    "status": "PASS",
                    "duration": elapsed,
                })
                print(f"\n[PASS] {name} (duration={format_duration(elapsed)})")

    except KeyboardInterrupt:
        interrupted = True
    except Exception as exc:
        failure_message = str(exc)

    elapsed = time.perf_counter() - pipeline_started
    finished_at = utc_now()

    print("\n" + "=" * 70)
    print("PIPELINE STEP SUMMARY")
    print("=" * 70)
    result_map = {str(item["name"]): item for item in results}
    for index, (name, _script, _validator) in enumerate(steps, start=1):
        item = result_map.get(name)
        if item is None:
            status = "SKIPPED"
            duration_text = ""
        else:
            status = str(item["status"])
            duration_text = f" ({format_duration(float(item['duration']))})"
        print(f"[{index}/{TOTAL_STEPS}] {name:<30} [{status}]{duration_text}")

    print("\n" + "=" * 70)
    if interrupted:
        print("PIPELINE INTERRUPTED")
    elif failure_message:
        print("PIPELINE FAILED")
    else:
        print("PIPELINE SUCCESS")
    print("=" * 70)
    print(f"Pipeline : {PIPELINE_NAME}")
    print(f"Run ID   : {run_id}")
    print(f"Started  : {started_at.isoformat()}")
    print(f"Finished : {finished_at.isoformat()}")
    print(f"Duration : {format_duration(elapsed)}")

    if failure_message:
        print(f"Error    : {failure_message}")
    if interrupted:
        print("Data layers after the interrupted step were not run.")
    elif failure_message:
        print("Remaining steps were skipped because an earlier step failed.")
    else:
        print("\nData flow:\n\nPostgreSQL\n    ↓\nBRONZE\n    ↓\nSILVER\n    ↓\nGOLD")

    print("=" * 70)

    if interrupted:
        return 130
    if failure_message:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
