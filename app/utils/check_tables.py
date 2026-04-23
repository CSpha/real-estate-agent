from sqlalchemy import text

from app.utils.db import get_engine


if __name__ == "__main__":
    engine = get_engine()

    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name;
    """

    with engine.connect() as conn:
        results = conn.execute(text(query))
        for row in results:
            print(row[0])