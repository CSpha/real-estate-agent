from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL


PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Settings:
    database_url: str | None
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str | None
    slack_webhook_url: str | None
    rentcast_api_key: str | None

    @property
    def sqlalchemy_database_url(self) -> str:
        if self.database_url:
            return self.database_url

        return URL.create(
            drivername="postgresql+psycopg2",
            username=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
        ).render_as_string(hide_password=False)


def _get_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    load_dotenv(PROJECT_ROOT / ".env")

    return Settings(
        database_url=os.getenv("DATABASE_URL"),
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=_get_int_env("DB_PORT", 5432),
        db_name=os.getenv("DB_NAME", "realestate"),
        db_user=os.getenv("DB_USER", "realestate"),
        db_password=os.getenv("DB_PASSWORD"),
        slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
        rentcast_api_key=os.getenv("RENTCAST_API_KEY"),
    )


def require_setting(value: str | None, name: str) -> str:
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value
