"""Bronze ingestion for the local SQL Server source system (MociSourceDB).

One module per source table (stores, products, customers, employees,
orders, order_items). Each module's `ingest()` function reads its table via
partitioned JDBC (through the ngrok tunnel to localhost:1433), appends the
result to the matching `<catalog>.bronze.<table>` Delta table, and updates
the table's watermark in the ingestion control table.
"""
