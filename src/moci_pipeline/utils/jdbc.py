"""Builds the JDBC URL (via the ngrok tunnel) and wraps spark.read.jdbc()
with partitioning hints for huge tables.

NOTE on testing: this module fetches connection secrets directly via
`dbutils.secrets`, which only exists inside a Databricks runtime — so
anything that touches `dbutils` or `SparkSession` here can only be
exercised by actually running on Databricks, not via local pytest (no
pyspark in requirements-dev.txt, by design — see discussion-notes.md).
`build_jdbc_url()` below is the one exception: it's pure string
formatting with no Spark/dbutils dependency, so if we ever want *some*
local unit-test coverage for this module, that's the function to start
with (it could be pytest-covered without adding pyspark at all).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

# Databricks secret scope created during the connectivity smoke test
# (see session-notes/discussion-notes.md section 11). Holds sql-host,
# sql-port, sql-user, sql-password.
SECRET_SCOPE = "moci-source-db"

# Not stored as a secret since it isn't sensitive — the mock source database
# name is effectively public knowledge within this project.
SOURCE_DATABASE = "MociSourceDB"

SQLSERVER_JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


def build_jdbc_url(host: str, port: str | int, database: str = SOURCE_DATABASE) -> str:
    """Builds a SQL Server JDBC connection URL.

    Pure string formatting — no Spark/dbutils dependency, safe to unit test.
    """
    return f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true;"


def _resolve_connection_params(
    spark: SparkSession, secret_scope: str = SECRET_SCOPE
) -> dict[str, str]:
    """Fetches the SQL Server connection details from the Databricks secret
    scope. Only callable inside a Databricks runtime.
    """
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)
    return {
        "host": dbutils.secrets.get(secret_scope, "sql-host"),
        "port": dbutils.secrets.get(secret_scope, "sql-port"),
        "user": dbutils.secrets.get(secret_scope, "sql-user"),
        "password": dbutils.secrets.get(secret_scope, "sql-password"),
    }


def read_table(
    spark: SparkSession,
    table: str | None,
    *,
    query: str | None = None,
    partition_column: str | None = None,
    lower_bound: int | None = None,
    upper_bound: int | None = None,
    num_partitions: int | None = None,
    secret_scope: str = SECRET_SCOPE,
) -> DataFrame:
    """Reads a SQL Server table via JDBC, through the ngrok tunnel.

    For huge tables, pass `partition_column`/`lower_bound`/`upper_bound`/
    `num_partitions` to enable parallel partitioned reads (keep
    `num_partitions` low, e.g. 4-8, since the ngrok tunnel is a single
    relay endpoint — see discussion-notes.md section 4). For small
    dimension tables, omit them for a single-connection read.

    Exactly one of `table` or `query` must be provided. If `query` is
    provided, it will be pushed down to SQL Server as a subquery source,
    allowing filters (such as watermark predicates) to execute at source.

    Args:
        spark: The active SparkSession (Databricks provides this
            automatically in notebooks/jobs).
        table: Source table name as `schema.table`, e.g. "sales.orders".
            Set to `None` when using `query`.
        query: SQL query text to execute on SQL Server; wrapped as
            `( <query> ) AS src` for JDBC reading.
        partition_column: Numeric/date column to partition the read on.
        lower_bound: Minimum value of partition_column to include.
        upper_bound: Maximum value of partition_column to include.
        num_partitions: Number of parallel JDBC connections to open.
        secret_scope: Databricks secret scope holding connection details.
    """
    conn = _resolve_connection_params(spark, secret_scope)
    jdbc_url = build_jdbc_url(conn["host"], conn["port"])
    properties = {
        "user": conn["user"],
        "password": conn["password"],
        "driver": SQLSERVER_JDBC_DRIVER,
    }

    if bool(table) == bool(query):
        raise ValueError("Provide exactly one of 'table' or 'query'.")

    source = table if query is None else f"( {query} ) AS src"

    if partition_column is not None:
        return spark.read.jdbc(
            url=jdbc_url,
            table=source,
            column=partition_column,
            lowerBound=lower_bound,
            upperBound=upper_bound,
            numPartitions=num_partitions,
            properties=properties,
        )
    return spark.read.jdbc(url=jdbc_url, table=source, properties=properties)
