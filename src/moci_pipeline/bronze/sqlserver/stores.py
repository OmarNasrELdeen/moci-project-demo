"""Bronze ingestion for sales.stores -> <catalog>.bronze.stores.

Small dimension table — single-connection JDBC read (no partitioning
needed), incremental via the ModifiedDate watermark.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from moci_pipeline.utils import watermark
from moci_pipeline.utils.bronze_writer import write_bronze
from moci_pipeline.utils.jdbc import read_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

SOURCE_TABLE = "sales.stores"
BRONZE_TABLE_NAME = "stores"


def ingest(spark: SparkSession, catalog: str) -> None:
    """Extracts new/changed rows from sales.stores since the last recorded
    watermark, appends them to <catalog>.bronze.stores, and advances the
    watermark to the max ModifiedDate seen in this batch.
    """
    watermark.ensure_control_table_exists(spark, catalog)
    last_watermark = watermark.get_last_watermark(spark, catalog, SOURCE_TABLE)

    watermark_sql = _to_sqlserver_datetime2_literal(last_watermark)
    query = (
        "SELECT StoreID, StoreCode, StoreName, Region, Country, OpenDate, "
        "CreatedDate, ModifiedDate "
        f"FROM {SOURCE_TABLE} "
        f"WHERE ModifiedDate > CAST('{watermark_sql}' AS DATETIME2(3))"
    )

    # Watermark predicate is pushed down to SQL Server via JDBC subquery,
    # so we avoid extracting the full table into Spark first.
    df = read_table(spark, table=None, query=query)
    if df.isEmpty():
        return

    write_bronze(df, catalog, BRONZE_TABLE_NAME, SOURCE_TABLE)

    new_watermark = _max_modified_date(df)
    watermark.update_watermark(spark, catalog, SOURCE_TABLE, new_watermark)


def _to_sqlserver_datetime2_literal(value: datetime) -> str:
    """Formats Python datetime into a SQL Server DATETIME2(3)-compatible
    UTC literal (YYYY-MM-DD HH:MM:SS.mmm).
    """
    as_utc = value.astimezone(datetime.UTC)
    return as_utc.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _max_modified_date(df):
    """Returns max(ModifiedDate) without importing pyspark functions.

    This keeps the transformation surface in this module minimal because the
    filtering now happens in SQL Server.
    """
    row = df.selectExpr("max(ModifiedDate) AS max_modified_date").collect()[0]
    return row["max_modified_date"]