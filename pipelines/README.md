# pipelines/

Lakeflow Declarative Pipeline (DLT) source, following Databricks' standard
Pipelines project layout.

```
pipelines/
├── transformations/   # the actual pipeline source (this IS the pipeline's DAG)
│   ├── silver/         # one file per Silver table (expectations, dedup, SCD 1/2)
│   └── gold/            # one file per Gold materialized view (aggregates/joins)
├── explorations/       # ad hoc notebooks over pipeline output — NOT part of the DAG
└── utilities/          # shared helper code (e.g. common SCD Type 2 merge logic),
                         # auto-importable by files under transformations/
```

Not built yet — placeholders only, until the Bronze ingestion module is
working end-to-end.
