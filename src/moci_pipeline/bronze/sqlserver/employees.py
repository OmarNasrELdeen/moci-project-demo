"""Bronze ingestion for sales.employees -> <catalog>.bronze.employees.

Small dimension table — single-connection JDBC read (no partitioning
needed), incremental via the ModifiedDate watermark.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from moci_pipeline.utils import watermark
from moci_pipeline.utils.bronze_writer import write_bronze
from moci_pipeline.utils.jdbc import read_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

SOURCE_TABLE = "sales.employees"
BRONZE_TABLE_NAME = "employees"


def ingest(spark: SparkSession, catalog: str) -> None:
    """Extracts new/changed rows from sales.employees since the last
    recorded watermark, appends to Bronze, and advances the watermark.
    """
    watermark.ensure_control_table_exists(spark, catalog)
    last_watermark = watermark.get_last_watermark(spark, catalog, SOURCE_TABLE)

    watermark_sql = _to_sqlserver_datetime2_literal(last_watermark)
    query = (
        "SELECT EmployeeID, StoreID, FullName, JobTitle, HireDate, CreatedDate, ModifiedDate "
        f"FROM {SOURCE_TABLE} "
        f"WHERE ModifiedDate > CAST('{watermark_sql}' AS DATETIME2(3))"
    )

    df = read_table(spark, table=None, query=query)
    if df.isEmpty():
        return

    write_bronze(df, catalog, BRONZE_TABLE_NAME, SOURCE_TABLE)
    watermark.update_watermark(spark, catalog, SOURCE_TABLE, _max_modified_date(df))


def _to_sqlserver_datetime2_literal(value: datetime) -> str:
    as_utc = value.astimezone(datetime.UTC)
    return as_utc.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _max_modified_date(df):
    row = df.selectExpr("max(ModifiedDate) AS max_modified_date").collect()[0]
    return row["max_modified_date"]
