from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold.top_products",
    comment="Top products by revenue and quantity from Silver order items.",
)
def top_products():
    order_items = spark.read.table("silver.order_items")
    products = spark.read.table("silver.products")

    return (
        order_items.join(products, on="ProductID", how="left")
        .groupBy("ProductID", "ProductCode", "ProductName", "Category")
        .agg(
            F.sum("Quantity").alias("units_sold"),
            F.sum("LineTotal").alias("revenue"),
        )
        .orderBy(F.desc("revenue"))
    )
