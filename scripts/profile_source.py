"""Inspect source schema/table counts before writing Silver mappings."""
from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, create_engine, func, select, inspect

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ROOT = Path(__file__).resolve().parents[1]


def safe_identifier(value: str, label: str) -> str:
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"Invalid {label}: {value!r}; only letters, digits and underscore are accepted")
    return value


def main() -> None:
    load_dotenv(ROOT / ".env")
    url = os.getenv("SOURCE_DATABASE_URL", "").strip()
    schema = os.getenv("SOURCE_SCHEMA", "finlens").strip()
    tables = [t.strip() for t in os.getenv(
        "SOURCE_TABLES",
        "symbols,sectors,stock_ohlcv_daily,index_ohlcv_daily,corporate_bonds",
    ).split(",") if t.strip()]
    if not url:
        raise SystemExit("Set SOURCE_DATABASE_URL in .env first (see .env.example).")

    safe_identifier(schema, "schema")
    for table_name in tables:
        safe_identifier(table_name, "table")

    engine = create_engine(url, pool_pre_ping=True)
    inspector = inspect(engine)
    available = set(inspector.get_table_names(schema=schema))
    rows: list[dict[str, object]] = []
    missing = [name for name in tables if name not in available]
    if missing:
        print(f"WARNING: tables not found in schema {schema!r}: {', '.join(missing)}")

    metadata = MetaData()
    with engine.connect() as conn:
        for table_name in tables:
            if table_name not in available:
                continue
            table = Table(table_name, metadata, schema=schema, autoload_with=engine)
            row_count = conn.execute(select(func.count()).select_from(table)).scalar_one()
            columns = inspector.get_columns(table_name, schema=schema)
            print(f"\nTABLE {schema}.{table_name} | rows={row_count}")
            for col in columns:
                nullable = col.get("nullable", True)
                dtype = str(col.get("type", "UNKNOWN"))
                default = col.get("default")
                print(f"  - {col['name']}: {dtype} | nullable={nullable} | default={default}")
                rows.append({
                    "schema_name": schema,
                    "table_name": table_name,
                    "row_count": int(row_count),
                    "column_name": col["name"],
                    "source_type": dtype,
                    "nullable": nullable,
                    "default_value": str(default) if default is not None else None,
                    "primary_key": col.get("primary_key", False),
                })

    report_dir = ROOT / "reports"
    report_dir.mkdir(exist_ok=True)
    report_path = report_dir / "source_profile.csv"
    pd.DataFrame(rows).to_csv(report_path, index=False, encoding="utf-8-sig")
    print(f"\nProfile written to: {report_path}")
    if missing:
        raise SystemExit("One or more configured tables were missing; correct SOURCE_SCHEMA/SOURCE_TABLES.")


if __name__ == "__main__":
    main()
