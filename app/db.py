import psycopg2
from psycopg2.extras import RealDictCursor

from app.utils.db import get_db_config


def get_connection():
    config = get_db_config()

    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
        cursor_factory=RealDictCursor,
    )
