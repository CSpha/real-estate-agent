from sqlalchemy.engine import make_url

from app.config import get_settings


def test_database_url_uses_one_db_variable_convention(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "database.example")
    monkeypatch.setenv("DB_PORT", "5433")
    monkeypatch.setenv("DB_NAME", "listings")
    monkeypatch.setenv("DB_USER", "agent")
    monkeypatch.setenv("DB_PASSWORD", "a password/with:symbols")
    get_settings.cache_clear()

    url = make_url(get_settings().sqlalchemy_database_url)

    assert url.drivername == "postgresql+psycopg2"
    assert url.host == "database.example"
    assert url.port == 5433
    assert url.database == "listings"
    assert url.username == "agent"
    assert url.password == "a password/with:symbols"
    get_settings.cache_clear()


def test_database_url_override_takes_precedence(monkeypatch):
    override = "postgresql+psycopg2://override:secret@db:5432/override_db"
    monkeypatch.setenv("DATABASE_URL", override)
    get_settings.cache_clear()

    assert get_settings().sqlalchemy_database_url == override
    get_settings.cache_clear()
