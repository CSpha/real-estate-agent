from __future__ import annotations

from functools import lru_cache

import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import Engine, create_engine

from app.config import get_settings
from app.utils.db import get_db_config


def get_connection() -> psycopg2.extensions.connection:
    config = get_db_config()

    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
        cursor_factory=RealDictCursor,
    )


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    return create_engine(
        get_settings().sqlalchemy_database_url,
        pool_pre_ping=True,
    )


def dispose_engine() -> None:
    if get_engine.cache_info().currsize:
        get_engine().dispose()
        get_engine.cache_clear()
