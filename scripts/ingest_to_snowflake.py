"""Append source-table snapshots to Snowflake Bronze with ETL metadata."""
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

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ROOT = Path(__file__).resolve().parents[1]


def safe_identifier(value: str, label: str) -> str:
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"Invalid {label}: {value!r}; only letters, digits and underscore are accepted")
    return value


def snowflake_connection() -> Any:
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [key for key in required if not os.getenv(key, "").strip()]
    if missing:
        raise SystemExit(f"Missing Snowflake settings in .env: {', '.join(missing)}")
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "FINLENS_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "FINLENS_DW"),
        schema="BRONZE",
        role=os.getenv("SNOWFLAKE_ROLE") or None,
        client_session_keep_alive=False,
    )


def normalized_column_names(columns: list[str]) -> list[str]:
    normalized = []
    seen = set()
    for col in columns:
        name = re.sub(r"[^A-Za-z0-9_]", "_", str(col)).strip("_").upper()
        if not name:
            name = "SOURCE_COLUMN"
        if name[0].isdigit():
            name = "C_" + name
        if name in seen:
            raise ValueError(f"Column name collision after normalization: {name}. Fix source names/mapping first.")
        seen.add(name)
        normalized.append(name)
    reserved = {"_ETL_BATCH_ID", "_ETL_LOADED_AT_UTC"}
    collisions = reserved.intersection(seen)
    if collisions:
        raise ValueError(f"Source contains reserved ETL metadata columns: {sorted(collisions)}")
    return normalized


def log_run(cur: Any, batch_id: str, table_name: str, status: str,
            source_rows: int | None, loaded_rows: int | None,
            started_at: datetime, finished_at: datetime,
            error_message: str | None = None) -> None:
    cur.execute(
        """INSERT INTO FINLENS_DW.OPS.ETL_RUN_LOG
           (RUN_ID, TABLE_NAME, STATUS, SOURCE_ROWS, LOADED_ROWS,
            STARTED_AT, FINISHED_AT, ERROR_MESSAGE)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (batch_id, table_name, status, source_rows, loaded_rows,
         started_at.replace(tzinfo=None), finished_at.replace(tzinfo=None), error_message),
    )


def main() -> int:
    load_dotenv(ROOT / ".env")
    source_url = os.getenv("SOURCE_DATABASE_URL", "").strip()
    source_schema = os.getenv("SOURCE_SCHEMA", "finlens").strip()
    table_names = [name.strip() for name in os.getenv(
        "SOURCE_TABLES",
        "symbols,sectors,stock_ohlcv_daily,index_ohlcv_daily,corporate_bonds",
    ).split(",") if name.strip()]
    row_limit_text = os.getenv("SOURCE_ROW_LIMIT", "").strip()
    row_limit = int(row_limit_text) if row_limit_text else None
    if row_limit is not None and row_limit < 1:
        raise SystemExit("SOURCE_ROW_LIMIT must be blank or an integer >= 1")
    if not source_url:
        raise SystemExit("Set SOURCE_DATABASE_URL in .env first (see .env.example).")

    safe_identifier(source_schema, "schema")
    for name in table_names:
        safe_identifier(name, "table")

    engine = create_engine(source_url, pool_pre_ping=True)
    sf = snowflake_connection()
    batch_id = str(uuid.uuid4())
    failed = False
    try:
        with sf.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS FINLENS_DW.BRONZE")
            cur.execute("CREATE SCHEMA IF NOT EXISTS FINLENS_DW.OPS")
            for table_name in table_names:
                started_at = datetime.now(timezone.utc)
                source_rows = None
                loaded_rows = 0
                try:
                    reflected = Table(table_name, MetaData(), schema=source_schema, autoload_with=engine)
                    stmt = select(reflected)
                    if row_limit:
                        stmt = stmt.limit(row_limit)
                    with engine.connect() as src_conn:
                        df = pd.read_sql_query(stmt, src_conn)
                    source_rows = len(df)
                    df.columns = normalized_column_names(list(df.columns))
                    df["_ETL_BATCH_ID"] = batch_id
                    # Naive UTC timestamp avoids mixing timezone-aware and naive values during inference.
                    df["_ETL_LOADED_AT_UTC"] = datetime.now(timezone.utc).replace(tzinfo=None)

                    if df.empty:
                        print(f"[SKIP] {table_name}: source returned 0 rows; no empty batch appended.")
                        log_run(cur, batch_id, table_name.upper(), "SUCCESS_EMPTY", 0, 0,
                                started_at, datetime.now(timezone.utc))
                        continue

                    success, nchunks, nrows, output = write_pandas(
                        conn=sf,
                        df=df,
                        table_name=table_name.upper(),
                        database=os.getenv("SNOWFLAKE_DATABASE", "FINLENS_DW").upper(),
                        schema="BRONZE",
                        chunk_size=100_000,
                        compression="snappy",
                        on_error="ABORT_STATEMENT",
                        parallel=4,
                        quote_identifiers=False,
                        auto_create_table=True,
                        overwrite=False,
                    )
                    loaded_rows = int(nrows or 0)
                    if not success or loaded_rows != len(df):
                        raise RuntimeError(f"write_pandas returned success={success}, chunks={nchunks}, rows={nrows}, details={output}")
                    finished_at = datetime.now(timezone.utc)
                    log_run(cur, batch_id, table_name.upper(), "SUCCESS", source_rows,
                            loaded_rows, started_at, finished_at)
                    print(f"[OK] {table_name}: read={source_rows:,}, loaded={loaded_rows:,}, batch={batch_id}")
                except Exception as exc:  # record failure per table, then continue for other tables
                    failed = True
                    finished_at = datetime.now(timezone.utc)
                    message = f"{type(exc).__name__}: {exc}"[:4000]
                    try:
                        log_run(cur, batch_id, table_name.upper(), "FAILED", source_rows,
                                loaded_rows, started_at, finished_at, message)
                    except Exception as audit_exc:
                        print(f"[AUDIT ERROR] {audit_exc}", file=sys.stderr)
                    print(f"[FAILED] {table_name}: {message}", file=sys.stderr)
    finally:
        sf.close()
        engine.dispose()

    print(f"\nBatch finished: {batch_id}; status={'PARTIAL_FAILURE' if failed else 'SUCCESS'}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
