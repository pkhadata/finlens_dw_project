import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
required = ["SOURCE_HOST","SOURCE_PORT","SOURCE_DBNAME","SOURCE_USER","SOURCE_PASSWORD","SOURCE_SCHEMA"]
missing = [x for x in required if not os.getenv(x)]
if missing:
    raise SystemExit("Missing .env values: " + ", ".join(missing))

url = (
    f"postgresql+psycopg://{os.environ['SOURCE_USER']}:{os.environ['SOURCE_PASSWORD']}"
    f"@{os.environ['SOURCE_HOST']}:{os.environ['SOURCE_PORT']}/{os.environ['SOURCE_DBNAME']}"
)
engine = create_engine(url, pool_pre_ping=True)

with engine.connect() as conn:
    print("PostgreSQL connection: OK")
    print("Server:", conn.execute(text("SELECT version()")).scalar())
    print("Schema:", os.environ["SOURCE_SCHEMA"])
