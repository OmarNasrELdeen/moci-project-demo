# Architecture & Design Notes

## Medallion pipeline

```
SQL Server (localhost, sales schema)
        │  JDBC batch read, incremental via ModifiedDate/ROWVERSION watermark
        │  (connection tunneled from Databricks serverless to localhost:1433)
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│ BRONZE  — plain modular PySpark job (Databricks Job / Workflow)       │
│  • Reads each source table via spark.read.jdbc with partitioned reads │
│    for the huge tables (orders, order_items)                          │
│  • Appends raw rows as-is (schema-on-read, extra metadata columns:     │
│    _ingested_at, _source_table)                                       │
│  • Writes to Unity Catalog: <catalog>.bronze.<table>  (Delta)          │
│  • Incremental: tracks last-seen watermark per table in a control      │
│    table (<catalog>.bronze._ingestion_control)                        │
└───────────────────────────────────────────────────────────────────────┘
        │  Delta table -> valid Structured Streaming source
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│ SILVER  — Lakeflow Declarative Pipelines (DLT)                        │
│  • Reads Bronze via dlt.read_stream                                    │
│  • Data quality expectations (@dlt.expect_or_drop / expect_or_fail)     │
│  • Dedup (latest record per business key using window/merge semantics) │
│  • Type casting, conforming column names, referential checks           │
│  • Writes to <catalog>.silver.<table>                                  │
└───────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│ GOLD    — Lakeflow Declarative Pipelines (DLT)                        │
│  • Joins Silver tables into business-level aggregates                  │
│    (e.g. sales by store/region/month, top products, customer LTV)      │
│  • Writes to <catalog>.gold.<table>                                    │
└───────────────────────────────────────────────────────────────────────┘
```

### Why JDBC batch for Bronze but DLT for Silver/Gold?

Lakeflow Declarative Pipelines does not drive ad-hoc JDBC batch extraction
from an external OLTP database as a first-class source. Once data lands in
Bronze as Delta, however, it becomes a fully valid Structured Streaming
source, so Silver/Gold benefit from DLT's declarative dependency graph,
built-in data quality expectations, autoscaling, and observability (event
log / lineage) without giving up control over the extraction step.

## Connectivity: Databricks Free Edition → local SQL Server

Databricks Free Edition runs serverless compute in Databricks-managed cloud
infrastructure — it cannot reach a machine's `localhost` network directly.
To let the Bronze JDBC job reach the local SQL Server instance for this
demo, we tunnel it out:

1. Local SQL Server listens on `localhost:1433` (default instance,
   Windows Authentication, TCP/IP enabled via SQL Server Configuration
   Manager).
2. An SSH/TCP tunnel (ngrok TCP tunnel, or Cloudflare Tunnel) exposes
   `localhost:1433` as a public `host:port` endpoint.
3. The Databricks Bronze job's JDBC connection string points at that
   tunnel endpoint instead of `localhost`, using **SQL Authentication**
   (a dedicated low-privilege SQL login, not Windows Auth, since Windows
   Auth doesn't work over a plain TCP tunnel from a non-domain-joined
   cloud VM).
4. Credentials are stored as a **Databricks secret scope**, never
   hard-coded — the JDBC URL/user/password are read via `dbutils.secrets`
   inside the Bronze job.

> This tunnel approach is for demo/mockup purposes only. In a real
> migration, the recommended pattern is a private network path (VPN /
> ExpressRoute / VNet peering) or Lakehouse Federation with a
> properly firewalled, dedicated replica — never exposing an OLTP
> primary directly.

**Setup steps (done when we reach the Bronze extraction step):**

1. Enable TCP/IP protocol for the SQL Server instance + restart the service.
2. Create a dedicated SQL login (least privilege: `db_datareader` on
   `MociSourceDB` only) for Databricks to use — do not tunnel in with
   Windows Auth / sa.
3. Start the tunnel (e.g. `ngrok tcp 1433`) and note the generated
   `host:port`.
4. Store `host`, `port`, `database`, `user`, `password` in a Databricks
   secret scope.
5. Verify connectivity from a Databricks notebook with a simple
   `spark.read.jdbc(...)` smoke test before building the full Bronze job.

## Unity Catalog layout

```
<catalog>              (e.g. "moci_demo")
├── bronze
│   ├── stores, products, customers, employees, orders, order_items
│   └── _ingestion_control   (per-table watermark tracking)
├── silver
│   ├── stores, products, customers, employees, orders, order_items
└── gold
    ├── sales_by_store_month
    ├── top_products
    └── customer_lifetime_value
```

## Incremental extraction strategy

Every source table carries `ModifiedDate` (updated on insert/update) and the
two fact tables also carry a `ROWVERSION` column. The Bronze job reads rows
where `ModifiedDate > @last_watermark` (falling back to a full load on first
run), then updates the control table with the max watermark seen. This
avoids re-reading all 5-10M+ rows of the fact tables on every run.
