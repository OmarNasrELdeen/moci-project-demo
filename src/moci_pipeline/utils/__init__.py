"""Shared utilities used by every table's Bronze ingestion module:

- `jdbc.py` — builds the JDBC URL (via the ngrok tunnel) and wraps
  `spark.read.jdbc()` with partitioning hints for huge tables.
- `watermark.py` — reads/writes the ingestion control table that tracks the
  last-seen watermark value per table (incremental ModifiedDate extraction).
- `bronze_writer.py` — generic Delta append writer that stamps every row
  with `_ingested_at` / `_source_table` metadata columns.
"""
