"""Databricks Bronze ingestion entrypoint.

Designed for Databricks Jobs where each table is a separate task:
- bronze_stores
- bronze_products
- bronze_customers
- bronze_employees
- bronze_orders
- bronze_order_items

Each task calls this same script with a different --table argument.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from moci_pipeline.bronze.sqlserver import (
    customers,
    employees,
    order_items,
    orders,
    products,
    stores,
)

TABLE_MODULES = {
    "stores": stores,
    "products": products,
    "customers": customers,
    "employees": employees,
    "orders": orders,
    "order_items": order_items,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze ingestion task")
    parser.add_argument("--catalog", default="moci_dev")
    parser.add_argument(
        "--table",
        required=True,
        choices=sorted(TABLE_MODULES.keys()),
        help="Logical Bronze table module to run, e.g. stores",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    module = TABLE_MODULES[args.table]
    print(
        f"[bronze] ingesting {module.SOURCE_TABLE} "
        f"-> {args.catalog}.bronze.{module.BRONZE_TABLE_NAME}"
    )
    module.ingest(spark, args.catalog)


if __name__ == "__main__":
    main()