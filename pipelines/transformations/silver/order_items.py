from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="order_items_bronze_cleaned",
    private=True,
    comment="Cleaned Bronze order-items records used internally by the pipeline.",
)
def order_items_bronze_cleaned():
    return (
        spark.readStream.table("bronze.order_items")
        .where(F.col("OrderItemID").isNotNull())
        .where(F.col("OrderID").isNotNull())
        .where(F.col("ProductID").isNotNull())
        .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    )


dp.create_streaming_table(
    name="silver.order_items",
    comment="Order items fact-like table as SCD Type 1 by OrderItemID.",
)


dp.create_auto_cdc_flow(
    target="silver.order_items",
    source="order_items_bronze_cleaned",
    keys=["OrderItemID"],
    sequence_by="ModifiedDate",
    stored_as_scd_type=1,
)
