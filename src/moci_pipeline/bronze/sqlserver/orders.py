"""Bronze ingestion for sales.orders -> <catalog>.bronze.orders.

Huge fact table — partitioned JDBC read, incremental via ModifiedDate
watermark.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from moci_pipeline.utils import watermark
from moci_pipeline.utils.bronze_writer import write_bronze
from moci_pipeline.utils.jdbc import read_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

SOURCE_TABLE = "sales.orders"
BRONZE_TABLE_NAME = "orders"
PARTITION_COLUMN = "OrderID"
LOWER_BOUND = 1
UPPER_BOUND = 5000000
NUM_PARTITIONS = 4


def ingest(spark: SparkSession, catalog: str) -> None:
    """Extracts new/changed rows from sales.orders since the last recorded
    watermark, appends to Bronze, and advances the watermark.
    """
    watermark.ensure_control_table_exists(spark, catalog)
    last_watermark = watermark.get_last_watermark(spark, catalog, SOURCE_TABLE)

    watermark_sql = _to_sqlserver_datetime2_literal(last_watermark)
    query = (
        "SELECT OrderID, CustomerID, StoreID, EmployeeID, OrderDate, Status, TotalAmount, "
        "CreatedDate, ModifiedDate, RowVersion "
        f"FROM {SOURCE_TABLE} "
        f"WHERE ModifiedDate > CAST('{watermark_sql}' AS DATETIME2(3))"
    )

    df = read_table(
        spark,
        table=None,
        query=query,
        partition_column=PARTITION_COLUMN,
        lower_bound=LOWER_BOUND,
        upper_bound=UPPER_BOUND,
        num_partitions=NUM_PARTITIONS,
    )
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
