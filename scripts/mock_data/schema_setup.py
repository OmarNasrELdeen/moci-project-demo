"""Creates the mock database, schema, and tables by running the DDL scripts
under sql/init and sql/ddl, in order.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .config import SqlServerSettings
from .db import get_connection, run_sql_script

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SQL_ROOT = _REPO_ROOT / "sql"

_INIT_SCRIPTS = [_SQL_ROOT / "init" / "01_create_database.sql"]
_DDL_SCRIPTS = [
    _SQL_ROOT / "ddl" / "02_create_schema.sql",
    _SQL_ROOT / "ddl" / "03_create_dimension_tables.sql",
    _SQL_ROOT / "ddl" / "04_create_fact_tables.sql",
]


def setup_schema(settings: SqlServerSettings) -> None:
    # CREATE DATABASE must run against 'master', outside the target database.
    with get_connection(settings, database="master") as conn:
        for script_path in _INIT_SCRIPTS:
            logger.info("Running %s", script_path.name)
            run_sql_script(conn, script_path.read_text(encoding="utf-8"))

    with get_connection(settings) as conn:
        for script_path in _DDL_SCRIPTS:
            logger.info("Running %s", script_path.name)
            run_sql_script(conn, script_path.read_text(encoding="utf-8"))
