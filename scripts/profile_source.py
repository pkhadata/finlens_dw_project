import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
schema = os.getenv("SOURCE_SCHEMA", "finlens")
tables = ["symbols","sectors","stock_ohlcv_daily","index_ohlcv_daily","corporate_bonds"]

url = (
    f"postgresql+psycopg://{os.environ['SOURCE_USER']}:{os.environ['SOURCE_PASSWORD']}"
    f"@{os.environ['SOURCE_HOST']}:{os.environ['SOURCE_PORT']}/{os.environ['SOURCE_DBNAME']}"
)
engine = create_engine(url, pool_pre_ping=True)
Path("reports").mkdir(exist_ok=True)

with engine.connect() as conn:
    available = {
        r[0] for r in conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema=:schema AND table_type='BASE TABLE'
        """), {"schema": schema})
    }
    missing = [t for t in tables if t not in available]
    if missing:
        raise SystemExit(f"Missing tables: {missing}")

    rows = []
    for table in tables:
        cols = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema=:schema AND table_name=:table
            ORDER BY ordinal_position
        """), {"schema": schema, "table": table}).fetchall()
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')).scalar()
        for col, dtype in cols:
            rows.append({"table_name": table, "column_name": col,
                         "data_type": dtype, "row_count": count})

df = pd.DataFrame(rows)
df.to_csv("reports/source_profile.csv", index=False)
print(df.to_string(index=False))
print("\nSaved: reports/source_profile.csv")
