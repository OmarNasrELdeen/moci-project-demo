# silver/

One `.py` file per Silver table, e.g. `customers.py`, `employees.py`,
`products.py`, `orders.py`, `order_items.py`. Each defines that table's
streaming table: reads its Bronze table via `dlt.read_stream`, applies data
quality expectations, dedup, and SCD Type 1 (overwrite) or Type 2
(historize with EffectiveStartDate/EffectiveEndDate/IsCurrent) logic as
appropriate for that table.

Not built yet.
