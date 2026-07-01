"""Populates the small dimension-like tables (stores, products, customers,
employees) using Faker. These are small enough (thousands of rows) that
generating them client-side and bulk-inserting with fast_executemany is fast
enough and gives us realistic-looking names/addresses/emails.
"""

from __future__ import annotations

import logging
import random

from faker import Faker
import pyodbc

from .config import SqlServerSettings
from .db import get_connection

logger = logging.getLogger(__name__)
_faker = Faker()

_CATEGORIES = [
    "Electronics",
    "Home & Kitchen",
    "Clothing",
    "Sports",
    "Toys",
    "Grocery",
    "Books",
    "Beauty",
]
_JOB_TITLES = ["Sales Associate", "Store Manager", "Cashier", "Inventory Clerk", "Assistant Manager"]


def _insert_rows(cursor: pyodbc.Cursor, sql: str, rows: list[tuple]) -> None:
    cursor.fast_executemany = True
    cursor.executemany(sql, rows)


def populate_stores(settings: SqlServerSettings, count: int) -> int:
    rows = [
        (
            f"ST{i + 1:05d}",
            f"{_faker.city()} Store #{i + 1}",
            _faker.state(),
            _faker.country(),
            _faker.date_between(start_date="-10y", end_date="-1y"),
        )
        for i in range(count)
    ]
    sql = (
        "INSERT INTO sales.stores (StoreCode, StoreName, Region, Country, OpenDate) "
        "VALUES (?, ?, ?, ?, ?)"
    )
    with get_connection(settings, autocommit=False) as conn:
        cursor = conn.cursor()
        _insert_rows(cursor, sql, rows)
        conn.commit()
    logger.info("Inserted %d stores", count)
    return count


def populate_products(settings: SqlServerSettings, count: int) -> int:
    rows = [
        (
            f"PRD{i + 1:06d}",
            _faker.catch_phrase(),
            random.choice(_CATEGORIES),
            round(random.uniform(2.5, 500.0), 2),
        )
        for i in range(count)
    ]
    sql = (
        "INSERT INTO sales.products (ProductCode, ProductName, Category, UnitPrice) "
        "VALUES (?, ?, ?, ?)"
    )
    with get_connection(settings, autocommit=False) as conn:
        cursor = conn.cursor()
        _insert_rows(cursor, sql, rows)
        conn.commit()
    logger.info("Inserted %d products", count)
    return count


def populate_customers(settings: SqlServerSettings, count: int) -> int:
    rows = []
    for i in range(count):
        first = _faker.first_name()
        last = _faker.last_name()
        rows.append(
            (
                first,
                last,
                f"{first.lower()}.{last.lower()}{i + 1}@example.com",
                _faker.phone_number()[:30],
                _faker.street_address()[:200],
                _faker.city(),
                _faker.country(),
            )
        )
    sql = (
        "INSERT INTO sales.customers (FirstName, LastName, Email, Phone, Address, City, Country) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    with get_connection(settings, autocommit=False) as conn:
        cursor = conn.cursor()
        _insert_rows(cursor, sql, rows)
        conn.commit()
    logger.info("Inserted %d customers", count)
    return count


def populate_employees(settings: SqlServerSettings, count: int, store_count: int) -> int:
    rows = [
        (
            random.randint(1, store_count),
            _faker.name(),
            random.choice(_JOB_TITLES),
            _faker.date_between(start_date="-8y", end_date="today"),
        )
        for _ in range(count)
    ]
    sql = (
        "INSERT INTO sales.employees (StoreID, FullName, JobTitle, HireDate) VALUES (?, ?, ?, ?)"
    )
    with get_connection(settings, autocommit=False) as conn:
        cursor = conn.cursor()
        _insert_rows(cursor, sql, rows)
        conn.commit()
    logger.info("Inserted %d employees", count)
    return count
