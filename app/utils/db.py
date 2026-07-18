from sqlalchemy import text

from app.db import get_engine

# Backward-compatible import path for older local scripts. New code should use
# ``from app.db import get_engine``.


if __name__ == "__main__":
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        for row in result:
            print(row[0])

    print("Database connection successful.")
