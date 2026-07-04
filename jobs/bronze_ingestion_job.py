"""Databricks Bronze ingestion entrypoint.

Designed for Databricks Jobs where each table is a separate task:
- bronze_stores
- bronze_products
- bronze_customers
- bronze_employees
- bronze_orders
- bronze_order_items

Each task calls this same script with a different --table argument.
The fact-table backfill tasks also pass --lower-bound / --upper-bound so
the initial runs load data in chunks instead of all at once.
"""

from __future__ import annotations

import argparse
import sys
import os
from pathlib import Path

from pyspark.sql import SparkSession

# Databricks executes this script from bundle files under .../files/jobs.
# Add .../files/src to sys.path so `moci_pipeline` imports resolve.
cwd = Path(os.getcwd())

possible_src_paths = [
    cwd / "src",
    cwd.parent / "src",
    cwd.parent.parent / "src",
    Path("/Workspace") / "src",
]

for path in possible_src_paths:
    if path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))

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
    parser.add_argument("--lower-bound", type=int, default=None)
    parser.add_argument("--upper-bound", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    module = TABLE_MODULES[args.table]
    print(
        f"[bronze] ingesting {module.SOURCE_TABLE} "
        f"-> {args.catalog}.bronze.{module.BRONZE_TABLE_NAME}"
    )
    if args.table in {"orders", "order_items"}:
        if args.lower_bound is None or args.upper_bound is None:
            raise ValueError("orders/order_items require --lower-bound and --upper-bound")
        module.ingest(
            spark,
            args.catalog,
            lower_bound=args.lower_bound,
            upper_bound=args.upper_bound,
        )
        return

    module.ingest(spark, args.catalog)


if __name__ == "__main__":
    main()