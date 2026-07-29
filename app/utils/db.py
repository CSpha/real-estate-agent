import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")


def _get_env(primary_name, fallback_name=None, default=None):
    value = os.getenv(primary_name)
    if value:
        return value

    if fallback_name:
        value = os.getenv(fallback_name)
        if value:
            return value

    return default


def get_db_config():
    return {
        "host": _get_env("DB_HOST", "POSTGRES_HOST", "localhost"),
        "port": _get_env("DB_PORT", "POSTGRES_PORT", "5432"),
        "dbname": _get_env("DB_NAME", "POSTGRES_DB", "realestate"),
        "user": _get_env("DB_USER", "POSTGRES_USER", "realestate"),
        "password": _get_env("DB_PASSWORD", "POSTGRES_PASSWORD"),
    }


def get_engine():
    config = get_db_config()

    connection_url = URL.create(
        "postgresql+psycopg2",
        username=config["user"],
        password=config["password"],
        host=config["host"],
        port=int(config["port"]),
        database=config["dbname"],
    )

    return create_engine(connection_url)


if __name__ == "__main__":
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        for row in result:
            print(row[0])

    print("Database connection successful.")
