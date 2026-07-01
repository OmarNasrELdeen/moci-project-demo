"""Thin pyodbc connection helpers shared by the mock data scripts."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager

import pyodbc

from .config import SqlServerSettings

_GO_SEPARATOR = re.compile(r"^\s*GO\s*$", re.IGNORECASE | re.MULTILINE)


def build_connection_string(settings: SqlServerSettings, database: str | None = None) -> str:
    parts = [
        f"DRIVER={{{settings.driver}}}",
        f"SERVER={settings.host},{settings.port}",
        f"DATABASE={database or settings.database}",
    ]
    if settings.trusted_connection:
        parts.append("Trusted_Connection=yes")
    else:
        parts.append(f"UID={settings.user}")
        parts.append(f"PWD={settings.password}")
    return ";".join(parts)


@contextmanager
def get_connection(
    settings: SqlServerSettings,
    database: str | None = None,
    autocommit: bool = True,
) -> Iterator[pyodbc.Connection]:
    conn = pyodbc.connect(build_connection_string(settings, database), autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()


def run_sql_script(conn: pyodbc.Connection, sql_text: str) -> None:
    """Executes a .sql file's contents, splitting on sqlcmd-style `GO` batch separators
    (pyodbc has no concept of GO; it must run each batch as a separate statement).
    """
    batches = [b.strip() for b in _GO_SEPARATOR.split(sql_text) if b.strip()]
    cursor = conn.cursor()
    for batch in batches:
        cursor.execute(batch)
    conn.commit()
