"""Populates the huge fact tables (orders, order_items) using set-based,
batched T-SQL generation that runs entirely server-side.

Row-by-row Python inserts would be far too slow for millions of rows, so
this pushes the random-data generation work to SQL Server itself using the
classic "cascading CTE" tally-table technique to materialize N rows cheaply.
"""

from __future__ import annotations

import logging
import math

from .config import SqlServerSettings
from .db import get_connection

logger = logging.getLogger(__name__)

_TALLY_CTE = """
;WITH L0 AS (SELECT 1 AS c UNION ALL SELECT 1),
L1 AS (SELECT 1 AS c FROM L0 a CROSS JOIN L0 b),
L2 AS (SELECT 1 AS c FROM L1 a CROSS JOIN L1 b),
L3 AS (SELECT 1 AS c FROM L2 a CROSS JOIN L2 b),
L4 AS (SELECT 1 AS c FROM L3 a CROSS JOIN L3 b),
L5 AS (SELECT 1 AS c FROM L4 a CROSS JOIN L4 b),
Nums AS (SELECT TOP (?) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM L5)
"""

_INSERT_ORDERS_BATCH = (
    _TALLY_CTE
    + """
INSERT INTO sales.orders (CustomerID, StoreID, EmployeeID, OrderDate, Status, TotalAmount)
OUTPUT inserted.OrderID INTO #NewOrders(OrderID)
SELECT
    (CHECKSUM(NEWID()) & 0x7FFFFFFF) % ? + 1,
    (CHECKSUM(NEWID()) & 0x7FFFFFFF) % ? + 1,
    (CHECKSUM(NEWID()) & 0x7FFFFFFF) % ? + 1,
    DATEADD(SECOND, -((CHECKSUM(NEWID()) & 0x7FFFFFFF) % 63072000), SYSUTCDATETIME()),
    CASE (CHECKSUM(NEWID()) & 0x7FFFFFFF) % 10
        WHEN 0 THEN 'Cancelled'
        WHEN 1 THEN 'Pending'
        ELSE 'Completed'
    END,
    0
FROM Nums;
"""
)

_INSERT_ORDER_ITEMS_FOR_BATCH = """
;WITH LineNums AS (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4)
INSERT INTO sales.order_items (OrderID, ProductID, Quantity, UnitPrice, LineTotal)
SELECT
    o.OrderID,
    pid.ProductID,
    q.Quantity,
    p.UnitPrice,
    p.UnitPrice * q.Quantity
FROM #NewOrders o
CROSS JOIN LineNums ln
CROSS APPLY (SELECT (CHECKSUM(NEWID()) & 0x7FFFFFFF) % ? + 1 AS ProductID) pid
CROSS APPLY (SELECT (CHECKSUM(NEWID()) & 0x7FFFFFFF) % 5 + 1 AS Quantity) q
INNER JOIN sales.products p ON p.ProductID = pid.ProductID
WHERE ln.n <= ((CHECKSUM(NEWID()) & 0x7FFFFFFF) % 4) + 1;
"""

_UPDATE_ORDER_TOTALS = """
UPDATE o
SET TotalAmount = agg.Total
FROM sales.orders o
INNER JOIN (
    SELECT OrderID, SUM(LineTotal) AS Total
    FROM sales.order_items
    WHERE OrderID IN (SELECT OrderID FROM #NewOrders)
    GROUP BY OrderID
) agg ON agg.OrderID = o.OrderID;
"""

_CREATE_TEMP_TABLE = "CREATE TABLE #NewOrders (OrderID BIGINT PRIMARY KEY);"
_DROP_TEMP_TABLE = "IF OBJECT_ID('tempdb..#NewOrders') IS NOT NULL DROP TABLE #NewOrders;"


def populate_orders_and_order_items(
    settings: SqlServerSettings,
    total_orders: int,
    batch_size: int,
    customer_count: int,
    store_count: int,
    employee_count: int,
    product_count: int,
) -> tuple[int, int]:
    """Generates `total_orders` rows in sales.orders (and their related
    sales.order_items, ~2-2.5 lines/order on average) in batches of
    `batch_size`, entirely server-side. Returns (orders_inserted, order_items_inserted).
    """
    num_batches = math.ceil(total_orders / batch_size)
    orders_inserted = 0
    order_items_inserted = 0

    with get_connection(settings, autocommit=False) as conn:
        cursor = conn.cursor()
        for batch_num in range(1, num_batches + 1):
            current_batch_size = min(batch_size, total_orders - orders_inserted)

            cursor.execute(_DROP_TEMP_TABLE)
            cursor.execute(_CREATE_TEMP_TABLE)
            cursor.execute(
                _INSERT_ORDERS_BATCH,
                (current_batch_size, customer_count, store_count, employee_count),
            )
            cursor.execute(_INSERT_ORDER_ITEMS_FOR_BATCH, (product_count,))
            cursor.execute(_UPDATE_ORDER_TOTALS)

            cursor.execute("SELECT COUNT(*) FROM #NewOrders;")
            new_orders = cursor.fetchone()[0]
            cursor.execute(
                "SELECT COUNT(*) FROM sales.order_items "
                "WHERE OrderID IN (SELECT OrderID FROM #NewOrders);"
            )
            new_items = cursor.fetchone()[0]
            cursor.execute(_DROP_TEMP_TABLE)
            conn.commit()

            orders_inserted += new_orders
            order_items_inserted += new_items
            logger.info(
                "Batch %d/%d: +%d orders, +%d order_items (total orders=%d, total items=%d)",
                batch_num,
                num_batches,
                new_orders,
                new_items,
                orders_inserted,
                order_items_inserted,
            )

    return orders_inserted, order_items_inserted
