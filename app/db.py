import os
from sqlalchemy import create_engine, text

def get_database_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return db_url

engine = create_engine(get_database_url(), pool_pre_ping=True)

def db_ping() -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
