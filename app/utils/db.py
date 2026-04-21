import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def get_engine():
    load_dotenv()

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    connection_string = (
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    return create_engine(connection_string)


if __name__ == "__main__":
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        for row in result:
            print(row[0])

    print("Database connection successful.")