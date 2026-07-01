"""Generic Delta append writer shared by every table's Bronze ingestion
module — stamps every row with _ingested_at / _source_table metadata
columns before appending to <catalog>.bronze.<table>.

NOTE on testing: like jdbc.py/watermark.py, this needs a live SparkSession
to do anything real, so it can only be exercised on Databricks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def write_bronze(df: DataFrame, catalog: str, bronze_table_name: str, source_table: str) -> None:
    """Appends `df` to `<catalog>.bronze.<bronze_table_name>`, stamping
    every row with ingestion metadata columns first.

    Args:
        df: The DataFrame just read from the source system (unchanged
            otherwise — Bronze is schema-on-read, no business logic here).
        catalog: Target Unity Catalog catalog, e.g. "moci_dev".
        bronze_table_name: Target table name under the bronze schema,
            e.g. "orders" (becomes <catalog>.bronze.orders).
        source_table: Fully-qualified source table name, e.g.
            "sales.orders" — recorded per row for traceability.
    """
    stamped = df.withColumn("_ingested_at", F.current_timestamp())\
                .withColumn("_source_table", F.lit(source_table))
    
    fqn = f"{catalog}.bronze.{bronze_table_name}"
    stamped.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(fqn)