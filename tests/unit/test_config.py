"""Unit tests for scripts.mock_data.config (no database connection required)."""

from __future__ import annotations

from mock_data.config import load_mock_data_volumes, load_sqlserver_settings


def test_load_sqlserver_settings_defaults(monkeypatch):
    monkeypatch.delenv("SQLSERVER_HOST", raising=False)
    monkeypatch.delenv("SQLSERVER_TRUSTED_CONNECTION", raising=False)

    settings = load_sqlserver_settings()

    assert settings.host == "localhost"
    assert settings.port == 1433
    assert settings.database == "MociSourceDB"
    assert settings.trusted_connection is True


def test_load_sqlserver_settings_sql_auth(monkeypatch):
    monkeypatch.setenv("SQLSERVER_TRUSTED_CONNECTION", "no")
    monkeypatch.setenv("SQLSERVER_USER", "demo_user")
    monkeypatch.setenv("SQLSERVER_PASSWORD", "demo_pass")

    settings = load_sqlserver_settings()

    assert settings.trusted_connection is False
    assert settings.user == "demo_user"
    assert settings.password == "demo_pass"


def test_load_mock_data_volumes_defaults(monkeypatch):
    for var in [
        "MOCK_CUSTOMERS_COUNT",
        "MOCK_PRODUCTS_COUNT",
        "MOCK_STORES_COUNT",
        "MOCK_EMPLOYEES_COUNT",
        "MOCK_ORDERS_COUNT",
        "MOCK_BATCH_SIZE",
    ]:
        monkeypatch.delenv(var, raising=False)

    volumes = load_mock_data_volumes()

    assert volumes.customers == 50000
    assert volumes.products == 5000
    assert volumes.stores == 200
    assert volumes.employees == 2000
    assert volumes.orders == 5000000
    assert volumes.batch_size == 100000
