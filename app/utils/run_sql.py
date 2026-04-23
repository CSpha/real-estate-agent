import sys
from pathlib import Path

from sqlalchemy import text

from app.utils.db import get_engine


def run_sql_file(file_path: str):
    sql_path = Path(file_path)

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    sql = sql_path.read_text()

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text(sql))

    print(f"Successfully ran: {file_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError("Usage: python -m app.utils.run_sql <sql_file_path>")

    run_sql_file(sys.argv[1])