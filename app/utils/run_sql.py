from pathlib import Path
from sqlalchemy import text

from app.utils.db import get_engine


def run_sql_file(file_path: str):
    engine = get_engine()
    sql = Path(file_path).read_text()

    with engine.begin() as conn:
        conn.execute(text(sql))

    print(f"Successfully ran: {file_path}")


if __name__ == "__main__":
    run_sql_file("sql/001_create_listings_current.sql")