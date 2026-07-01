"""Configuration loader for the mock data generation scripts.

Reads connection details and mock data volumes from environment variables
(populated from a local .env file via python-dotenv).
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class SqlServerSettings:
    host: str
    port: int
    database: str
    driver: str
    trusted_connection: bool
    user: str
    password: str


@dataclass(frozen=True)
class MockDataVolumes:
    customers: int
    products: int
    stores: int
    employees: int
    orders: int
    order_items_multiplier: int
    batch_size: int


def load_sqlserver_settings() -> SqlServerSettings:
    return SqlServerSettings(
        host=os.getenv("SQLSERVER_HOST", "localhost"),
        port=int(os.getenv("SQLSERVER_PORT", "1433")),
        database=os.getenv("SQLSERVER_DATABASE", "MociSourceDB"),
        driver=os.getenv("SQLSERVER_DRIVER", "ODBC Driver 17 for SQL Server"),
        trusted_connection=os.getenv("SQLSERVER_TRUSTED_CONNECTION", "yes").lower() == "yes",
        user=os.getenv("SQLSERVER_USER", ""),
        password=os.getenv("SQLSERVER_PASSWORD", ""),
    )


def load_mock_data_volumes() -> MockDataVolumes:
    return MockDataVolumes(
        customers=int(os.getenv("MOCK_CUSTOMERS_COUNT", "50000")),
        products=int(os.getenv("MOCK_PRODUCTS_COUNT", "5000")),
        stores=int(os.getenv("MOCK_STORES_COUNT", "200")),
        employees=int(os.getenv("MOCK_EMPLOYEES_COUNT", "2000")),
        orders=int(os.getenv("MOCK_ORDERS_COUNT", "5000000")),
        order_items_multiplier=int(os.getenv("MOCK_ORDER_ITEMS_MULTIPLIER", "2")),
        batch_size=int(os.getenv("MOCK_BATCH_SIZE", "100000")),
    )
