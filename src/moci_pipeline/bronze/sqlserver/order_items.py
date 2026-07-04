"""Bronze ingestion for sales.order_items -> <catalog>.bronze.order_items.

Huge fact table — partitioned JDBC read by OrderItemID range for the
initial backfill chunks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from moci_pipeline.utils.bronze_writer import write_bronze
from moci_pipeline.utils.jdbc import read_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

SOURCE_TABLE = "sales.order_items"
BRONZE_TABLE_NAME = "order_items"
PARTITION_COLUMN = "OrderItemID"
NUM_PARTITIONS = 4


def ingest(
    spark: SparkSession,
    catalog: str,
    *,
    lower_bound: int,
    upper_bound: int,
) -> None:
    """Extracts a chunk of sales.order_items by OrderItemID range and
    appends it to Bronze.
    """
    if lower_bound > upper_bound:
        raise ValueError("lower_bound must be <= upper_bound")

    query = (
        "SELECT OrderItemID, OrderID, ProductID, Quantity, UnitPrice, LineTotal, "
        "CreatedDate, ModifiedDate, RowVersion "
        f"FROM {SOURCE_TABLE} "
        f"WHERE OrderItemID BETWEEN {lower_bound} AND {upper_bound}"
    )

    df = read_table(
        spark,
        table=None,
        query=query,
        partition_column=PARTITION_COLUMN,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        num_partitions=NUM_PARTITIONS,
    )
    if df.isEmpty():
        return

    write_bronze(df, catalog, BRONZE_TABLE_NAME, SOURCE_TABLE)
