"""Load PostgreSQL snapshots into Snowflake Bronze safely."""
from __future__ import annotations

import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.engine import URL


# ============================================================
# CONFIG
# ============================================================

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ROOT = Path(__file__).resolve().parents[1]
DATABASE = "FINLENS_DW"
BRONZE_SCHEMA = "BRONZE"
OPS_SCHEMA = "OPS"
PIPELINE_NAME = "FINLENS_BRONZE_INGESTION"


# ============================================================
# HELPERS
# ============================================================

def safe_identifier(value: str, label: str) -> str:
    """Validate identifiers used in SQL statements."""

    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"Invalid {label}: {value!r}. "
            "Only letters, digits and underscores are accepted."
        )

    return value


def snowflake_connection() -> Any:
    """Connect to Snowflake using the existing .env settings."""

    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
    ]

    missing = [
        key
        for key in required
        if not os.getenv(key, "").strip()
    ]

    if missing:
        raise RuntimeError(
            "Missing Snowflake settings in .env: "
            + ", ".join(missing)
        )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.getenv(
            "SNOWFLAKE_WAREHOUSE", "FINLENS_WH"
        ),
        database=os.getenv(
            "SNOWFLAKE_DATABASE", DATABASE
        ),
        schema=BRONZE_SCHEMA,
        role=os.getenv("SNOWFLAKE_ROLE") or None,
        client_session_keep_alive=False,
    )


def create_postgresql_engine() -> Any:
    """Build PostgreSQL connection from SOURCE_* variables."""

    host = os.getenv("SOURCE_HOST", "").strip()
    port_text = os.getenv("SOURCE_PORT", "5432").strip()
    dbname = os.getenv("SOURCE_DBNAME", "").strip()
    user = os.getenv("SOURCE_USER", "").strip()
    password = os.getenv("SOURCE_PASSWORD", "")

    required = {
        "SOURCE_HOST": host,
        "SOURCE_PORT": port_text,
        "SOURCE_DBNAME": dbname,
        "SOURCE_USER": user,
        "SOURCE_PASSWORD": password,
    }

    missing = [
        key
        for key, value in required.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Missing PostgreSQL settings in .env: "
            + ", ".join(missing)
        )

    try:
        port = int(port_text)
    except ValueError as exc:
        raise RuntimeError(
            "SOURCE_PORT must be a valid integer."
        ) from exc

    connection_url = URL.create(
        drivername="postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=port,
        database=dbname,
    )

    return create_engine(
        connection_url,
        pool_pre_ping=True,
    )


def normalized_column_names(columns: list[str]) -> list[str]:
    """
    Normalize PostgreSQL names to uppercase Snowflake identifiers.

    Keep the metadata naming convention already used in Bronze:
        _BATCH_ID
        _LOADED_AT
        _SOURCE_SYSTEM
    """

    normalized = []
    seen = set()

    for column in columns:
        name = re.sub(
            r"[^A-Za-z0-9_]",
            "_",
            str(column),
        ).strip("_").upper()

        if not name:
            name = "SOURCE_COLUMN"

        if name[0].isdigit():
            name = "C_" + name

        if name in seen:
            raise ValueError(
                f"Duplicate column after normalization: {name}"
            )

        seen.add(name)
        normalized.append(name)

    reserved = {
        "_BATCH_ID",
        "_LOADED_AT",
        "_SOURCE_SYSTEM",
    }

    collisions = reserved.intersection(seen)

    if collisions:
        raise ValueError(
            "Source has columns reserved for ETL metadata: "
            f"{sorted(collisions)}"
        )

    return normalized


def ensure_control_tables(cur: Any) -> None:
    """Ensure schemas and the existing ETL audit table are present."""

    cur.execute(
        f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{BRONZE_SCHEMA}"
    )

    cur.execute(
        f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{OPS_SCHEMA}"
    )

    # Match the ETL_RUN_LOG structure created during initialization.
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS
            {DATABASE}.{OPS_SCHEMA}.ETL_RUN_LOG
        (
            RUN_ID STRING,
            PIPELINE_NAME STRING,
            TABLE_NAME STRING,
            STARTED_AT TIMESTAMP_TZ,
            FINISHED_AT TIMESTAMP_TZ,
            STATUS STRING,
            ROWS_LOADED NUMBER,
            ERROR_MESSAGE STRING
        )
        """
    )


def log_run(
    cur: Any,
    batch_id: str,
    table_name: str,
    status: str,
    rows_loaded: int,
    started_at: datetime,
    finished_at: datetime,
    error_message: str | None = None,
) -> None:
    """Insert one table-level audit record."""

    cur.execute(
        f"""
        INSERT INTO {DATABASE}.{OPS_SCHEMA}.ETL_RUN_LOG
        (
            RUN_ID,
            PIPELINE_NAME,
            TABLE_NAME,
            STARTED_AT,
            FINISHED_AT,
            STATUS,
            ROWS_LOADED,
            ERROR_MESSAGE
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            batch_id,
            PIPELINE_NAME,
            table_name,
            started_at,
            finished_at,
            status,
            rows_loaded,
            error_message,
        ),
    )


def utc_now() -> datetime:
    """Return timezone-aware UTC time."""

    return datetime.now(timezone.utc)


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    load_dotenv(ROOT / ".env")

    source_schema = os.getenv(
        "SOURCE_SCHEMA", "finlens"
    ).strip()

    table_names = [
        name.strip()
        for name in os.getenv(
            "SOURCE_TABLES",
            (
                "symbols,"
                "sectors,"
                "stock_ohlcv_daily,"
                "index_ohlcv_daily,"
                "corporate_bonds"
            ),
        ).split(",")
        if name.strip()
    ]

    # This version refreshes complete source snapshots.
    # A partial source read must not replace the full Bronze table.
    row_limit_text = os.getenv(
        "SOURCE_ROW_LIMIT", ""
    ).strip()

    if row_limit_text:
        raise RuntimeError(
            "SOURCE_ROW_LIMIT is not supported for full snapshot "
            "refresh. Clear SOURCE_ROW_LIMIT in .env before running."
        )

    if not table_names:
        raise RuntimeError(
            "No source tables configured in SOURCE_TABLES."
        )

    safe_identifier(source_schema, "schema")

    for table_name in table_names:
        safe_identifier(table_name, "table")

    print()
    print("=" * 70)
    print("FINLENS DW - BRONZE INGESTION")
    print("=" * 70)

    engine = None
    sf = None

    try:
        print("\nConnecting to PostgreSQL...")

        engine = create_postgresql_engine()

        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")

        print("PostgreSQL connection: OK")

        print("Connecting to Snowflake...")

        sf = snowflake_connection()

        print("Snowflake connection: OK")

    except Exception as exc:
        print(
            f"\n[CONNECTION FAILED] "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

        if sf is not None:
            sf.close()

        if engine is not None:
            engine.dispose()

        return 1

    batch_id = str(uuid.uuid4())
    failed = False

    print(f"\nBatch ID: {batch_id}")
    print(f"Source schema: {source_schema}")
    print(f"Source tables: {', '.join(table_names)}")

    try:
        with sf.cursor() as cur:
            ensure_control_tables(cur)

            for table_name in table_names:
                target_name = table_name.upper()

                # A separate staging table is used for each source table.
                # It prevents an unsuccessful upload from replacing
                # the existing Bronze snapshot.
                stage_suffix = (
                    batch_id.replace("-", "")[:8].upper()
                )

                stage_name = (
                    f"_STG_{target_name}_{stage_suffix}"
                )

                safe_identifier(stage_name, "staging table")

                started_at = utc_now()
                loaded_rows = 0
                stage_created = False

                print("\n" + "-" * 70)
                print(
                    f"Processing: "
                    f"{source_schema}.{table_name}"
                )

                try:
                    # ------------------------------------------------
                    # 1. Read source table
                    # ------------------------------------------------

                    reflected = Table(
                        table_name,
                        MetaData(),
                        schema=source_schema,
                        autoload_with=engine,
                    )

                    stmt = select(reflected)

                    with engine.connect() as src_conn:
                        df = pd.read_sql_query(
                            stmt,
                            src_conn,
                        )

                    source_rows = len(df)

                    print(
                        f"Source rows: {source_rows:,}"
                    )

                    # Avoid clearing a target table when the source
                    # unexpectedly returns zero rows.
                    if df.empty:
                        log_run(
                            cur=cur,
                            batch_id=batch_id,
                            table_name=target_name,
                            status="SKIPPED_EMPTY",
                            rows_loaded=0,
                            started_at=started_at,
                            finished_at=utc_now(),
                            error_message=(
                                "Source table is empty; "
                                "existing Bronze snapshot left unchanged."
                            ),
                        )

                        print(
                            f"[SKIP] {table_name}: empty source; "
                            "existing Bronze data unchanged."
                        )
                        continue

                    # ------------------------------------------------
                    # 2. Normalize columns and add metadata
                    # ------------------------------------------------

                    df.columns = normalized_column_names(
                        list(df.columns)
                    )

                    df["_BATCH_ID"] = batch_id

                    # Bronze metadata uses a UTC-naive timestamp
                    # in the existing table's _LOADED_AT column.
                    df["_LOADED_AT"] = (
                        utc_now().replace(tzinfo=None)
                    )

                    df["_SOURCE_SYSTEM"] = "POSTGRESQL"

                    # ------------------------------------------------
                    # 3. Create staging table from existing target
                    # ------------------------------------------------

                    cur.execute(
                        f"""
                        CREATE TEMPORARY TABLE
                            {DATABASE}.{BRONZE_SCHEMA}.{stage_name}
                        LIKE
                            {DATABASE}.{BRONZE_SCHEMA}.{target_name}
                        """
                    )

                    stage_created = True

                    # ------------------------------------------------
                    # 4. Upload the full dataset to staging
                    # ------------------------------------------------

                    success, nchunks, nrows, output = (
                        write_pandas(
                            conn=sf,
                            df=df,
                            table_name=stage_name,
                            database=DATABASE,
                            schema=BRONZE_SCHEMA,
                            chunk_size=100_000,
                            compression="snappy",
                            on_error="ABORT_STATEMENT",
                            parallel=4,
                            quote_identifiers=False,
                            auto_create_table=False,
                            overwrite=False,
                            use_logical_type=True,
                        )
                    )

                    staged_rows = int(nrows or 0)

                    if (
                        not success
                        or staged_rows != len(df)
                    ):
                        raise RuntimeError(
                            "Staging upload failed: "
                            f"success={success}, "
                            f"chunks={nchunks}, "
                            f"rows={nrows}, "
                            f"details={output}"
                        )

                    # ------------------------------------------------
                    # 5. Atomically replace the Bronze snapshot
                    # ------------------------------------------------

                    cur.execute(
                        f"""
                        INSERT OVERWRITE INTO
                            {DATABASE}.{BRONZE_SCHEMA}.{target_name}
                        SELECT *
                        FROM
                            {DATABASE}.{BRONZE_SCHEMA}.{stage_name}
                        """
                    )

                    loaded_rows = staged_rows

                    # ------------------------------------------------
                    # 6. Audit success
                    # ------------------------------------------------

                    log_run(
                        cur=cur,
                        batch_id=batch_id,
                        table_name=target_name,
                        status="SUCCESS",
                        rows_loaded=loaded_rows,
                        started_at=started_at,
                        finished_at=utc_now(),
                    )

                    print(
                        f"[OK] {table_name}: "
                        f"read={source_rows:,}, "
                        f"loaded={loaded_rows:,}, "
                        "snapshot replaced successfully."
                    )

                except Exception as exc:
                    failed = True

                    message = (
                        f"{type(exc).__name__}: {exc}"
                    )[:4000]

                    try:
                        log_run(
                            cur=cur,
                            batch_id=batch_id,
                            table_name=target_name,
                            status="FAILED",
                            rows_loaded=loaded_rows,
                            started_at=started_at,
                            finished_at=utc_now(),
                            error_message=message,
                        )

                    except Exception as audit_exc:
                        print(
                            f"[AUDIT ERROR] {audit_exc}",
                            file=sys.stderr,
                        )

                    print(
                        f"[FAILED] {table_name}: {message}",
                        file=sys.stderr,
                    )

                finally:
                    if stage_created:
                        try:
                            cur.execute(
                                f"""
                                DROP TABLE IF EXISTS
                                    {DATABASE}.{BRONZE_SCHEMA}.{stage_name}
                                """
                            )
                        except Exception as cleanup_exc:
                            print(
                                f"[CLEANUP WARNING] {stage_name}: "
                                f"{cleanup_exc}",
                                file=sys.stderr,
                            )

    except Exception as exc:
        failed = True

        print(
            f"\n[FATAL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

    finally:
        if sf is not None:
            sf.close()

        if engine is not None:
            engine.dispose()

    status = "PARTIAL_FAILURE" if failed else "SUCCESS"

    print("\n" + "=" * 70)
    print(
        f"Batch finished: {batch_id}; status={status}"
    )
    print("=" * 70)

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())