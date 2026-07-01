"""Reads/writes the ingestion control table
(<catalog>.bronze._ingestion_control) that tracks the last-seen watermark
value per table, for incremental ModifiedDate-based extraction.

NOTE on testing: like jdbc.py, this module needs a live SparkSession /
Unity Catalog table to do anything real, so it can only be exercised on
Databricks — not via local pytest (no pyspark locally, by design).
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

CONTROL_TABLE_NAME = "_ingestion_control"

# Default watermark used the first time a table is ingested (i.e. "no prior
# run exists yet") — far enough in the past to include all historical rows.
EPOCH_WATERMARK = datetime(1900, 1, 1, tzinfo=datetime.UTC)


def _control_table_fqn(catalog: str) -> str:
    """Fully-qualified name of the ingestion control table for a catalog."""
    return f"{catalog}.bronze.{CONTROL_TABLE_NAME}"


def ensure_control_table_exists(spark: SparkSession, catalog: str) -> None:
    """Creates the ingestion control table if it doesn't already exist.

    Columns:
        table_name (STRING, PK)  — e.g. "sales.orders"
        last_watermark (TIMESTAMP) — max ModifiedDate successfully ingested
        last_run_at (TIMESTAMP)     — when this watermark was recorded
    """
    fqn = _control_table_fqn(catalog)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            table_name STRING NOT NULL,
            last_watermark TIMESTAMP NOT NULL,
            last_run_at TIMESTAMP NOT NULL
        ) USING DELTA
        """
    )


def get_last_watermark(spark: SparkSession, catalog: str, table_name: str) -> datetime:
    """Returns the last recorded watermark for `table_name`, or
    `EPOCH_WATERMARK` if this table has never been ingested before.
    """
    fqn = _control_table_fqn(catalog)
    rows = (
        spark.sql(f"SELECT last_watermark FROM {fqn} WHERE table_name = :t", args={"t": table_name})
        .collect()
    )
    if not rows:
        return EPOCH_WATERMARK
    return rows[0]["last_watermark"]


def update_watermark(
    spark: SparkSession, catalog: str, table_name: str, new_watermark: datetime
) -> None:
    """Upserts the watermark for `table_name` after a successful ingest."""
    fqn = _control_table_fqn(catalog)
    now = datetime.now(datetime.UTC)
    spark.sql(
        f"""
        MERGE INTO {fqn} AS target
        USING (SELECT :t AS table_name, :w AS last_watermark, :now AS last_run_at) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET
            target.last_watermark = source.last_watermark,
            target.last_run_at = source.last_run_at
        WHEN NOT MATCHED THEN INSERT (table_name, last_watermark, last_run_at)
            VALUES (source.table_name, source.last_watermark, source.last_run_at)
        """,
        args={"t": table_name, "w": new_watermark, "now": now},
    )