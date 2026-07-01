from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold.customer_lifetime_value",
    comment="Lifetime value metrics per customer from Silver orders.",
)
def customer_lifetime_value():
    orders = spark.read.table("silver.orders")
    customers = spark.read.table("silver.customers")

    return (
        orders.join(customers, on="CustomerID", how="left")
        .groupBy("CustomerID", "FirstName", "LastName", "Email", "Country")
        .agg(
            F.countDistinct("OrderID").alias("order_count"),
            F.sum("TotalAmount").alias("lifetime_value"),
            F.max("OrderDate").alias("last_order_at"),
        )
    )
