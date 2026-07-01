"""CLI entrypoint: creates the mock SQL Server database/schema/tables and
populates them with mock data (small dimension tables via Faker, huge fact
tables via set-based batched T-SQL).

Usage:
    python scripts/run_mock_data_setup.py [--skip-schema] [--skip-dimensions] [--skip-facts]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from mock_data import dimensions, facts, schema_setup  # noqa: E402
from mock_data.config import load_mock_data_volumes, load_sqlserver_settings  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-schema", action="store_true", help="Skip database/table creation")
    parser.add_argument(
        "--skip-dimensions", action="store_true", help="Skip dimension table population"
    )
    parser.add_argument("--skip-facts", action="store_true", help="Skip fact table population")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_sqlserver_settings()
    volumes = load_mock_data_volumes()

    start = time.time()

    if not args.skip_schema:
        logger.info("Creating database/schema/tables...")
        schema_setup.setup_schema(settings)

    if not args.skip_dimensions:
        logger.info("Populating dimension tables...")
        dimensions.populate_stores(settings, volumes.stores)
        dimensions.populate_products(settings, volumes.products)
        dimensions.populate_customers(settings, volumes.customers)
        dimensions.populate_employees(settings, volumes.employees, volumes.stores)

    if not args.skip_facts:
        logger.info("Populating fact tables (orders, order_items)...")
        orders, order_items = facts.populate_orders_and_order_items(
            settings,
            total_orders=volumes.orders,
            batch_size=volumes.batch_size,
            customer_count=volumes.customers,
            store_count=volumes.stores,
            employee_count=volumes.employees,
            product_count=volumes.products,
        )
        logger.info("Fact generation complete: %d orders, %d order_items", orders, order_items)

    elapsed = time.time() - start
    logger.info("Mock data setup complete in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
