"""Bronze extraction/load package: one subfolder per source system.

Each source-system subfolder (e.g. `sqlserver/`) contains one Python module
per source table. Each module exposes an `ingest()` function that performs
the full extract (JDBC read) + load (Delta append write) for that table,
using the shared helpers in `moci_pipeline.utils`.

Keeping extract+load together per table (rather than generic cross-table
extract/ and load/ layers) keeps each table's ingestion logic self
contained and easy to review/test in isolation.
"""
