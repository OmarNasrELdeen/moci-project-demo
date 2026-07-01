# Moci Project Demo — SQL Server → Databricks Medallion Migration

End-to-end demo migrating multiple tables from an on-premises SQL Server instance into
Databricks (Free Edition, Unity Catalog, serverless compute), following the
**Bronze → Silver → Gold** medallion architecture.

## Architecture

```
SQL Server (localhost)
        │  JDBC (batch, incremental via watermark) over secure tunnel
        ▼
  Bronze  (raw, append-only, Delta)      — plain modular PySpark job
        │  Structured Streaming read
        ▼
  Silver  (cleaned, conformed, deduped)  — Lakeflow Declarative Pipelines (DLT)
        │
        ▼
  Gold    (business-level aggregates)    — Lakeflow Declarative Pipelines (DLT)
```

**Why this split:** Lakeflow Declarative Pipelines (DLT) does not natively drive JDBC
batch extraction from an external database, so Bronze ingestion is a standalone,
testable PySpark module. Once data lands in Bronze as Delta, it becomes a valid
streaming source, so Silver and Gold are implemented as declarative pipelines with
built-in data quality expectations.

## Repository layout

```
├── src/moci_pipeline/       # Modular Python package
│   ├── bronze/sqlserver/   # One module per source table: extract (JDBC) + load (Delta)
│   └── utils/               # Shared helpers: JDBC reader, watermark control table, Delta writer
├── jobs/                    # Bronze ingestion job entrypoints (Databricks Jobs, plain .py scripts)
├── pipelines/               # Lakeflow Declarative Pipeline source (Silver/Gold)
│   ├── transformations/     # The pipeline DAG: transformations/silver/*.py, transformations/gold/*.py
│   ├── explorations/        # Ad hoc notebooks over pipeline output — not part of the DAG
│   └── utilities/           # Shared helpers for transformations/ (e.g. SCD Type 2 merge logic)
├── sql/                     # SQL Server DDL + mock data generation SQL
├── scripts/                 # One-off / setup scripts (mock data orchestration)
├── tests/                   # pytest unit tests
├── resources/               # Databricks Asset Bundle resources (jobs/pipelines yml) — added later
└── databricks.yml           # Databricks Asset Bundle root config — added later
```

**Why one file per table (not generic extract/ and load/ layers):** each Bronze table's
extract (partitioned JDBC read) and load (Delta append write) logic lives together in one
file, calling into shared `utils/` helpers. This keeps each table's ingestion
self-contained and easy to review/test in isolation, rather than requiring the job to
wire together separate generic extract and load modules per table. `sqlserver/` is a
per-source-system folder — a second source system would get its own sibling folder.

## Status

- [x] Approach agreed (medallion architecture, JDBC bronze + DLT silver/gold, Unity Catalog)
- [ ] Mock source data generated in local SQL Server
- [ ] Tunnel connectivity from Databricks to local SQL Server
- [ ] Bronze ingestion module
- [ ] Silver/Gold declarative pipelines
- [ ] Databricks Asset Bundles + CI/CD

## Local prerequisites

- SQL Server 2022 (Developer/Express), default instance, Windows Authentication
- Python 3.11+ (project tested on 3.13)
- ODBC Driver 17 for SQL Server

## Getting started (mock data)

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env   # adjust values if needed
python scripts/run_mock_data_setup.py
```

See [sql/README.md](sql/README.md) for the source schema and [docs/architecture.md](docs/architecture.md)
for full design notes.
