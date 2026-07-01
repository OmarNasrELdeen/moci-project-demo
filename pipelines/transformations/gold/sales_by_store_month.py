from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold.sales_by_store_month",
    comment="Monthly sales by store from Silver orders.",
)
def sales_by_store_month():
    orders = spark.read.table("silver.orders")
    stores = spark.read.table("silver.stores")

    return (
        orders.join(stores, on="StoreID", how="left")
        .withColumn("order_month", F.date_trunc("month", F.col("OrderDate")))
        .groupBy("StoreID", "StoreCode", "StoreName", "Region", "Country", "order_month")
        .agg(
            F.countDistinct("OrderID").alias("order_count"),
            F.sum("TotalAmount").alias("gross_sales"),
        )
    )
