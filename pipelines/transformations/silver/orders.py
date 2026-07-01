from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="orders_bronze_cleaned",
    private=True,
    comment="Cleaned Bronze orders records used internally by the pipeline.",
)
def orders_bronze_cleaned():
    return (
        spark.readStream.table("bronze.orders")
        .where(F.col("OrderID").isNotNull())
        .where(F.col("CustomerID").isNotNull())
        .where(F.col("StoreID").isNotNull())
        .withColumn("OrderDate", F.to_timestamp("OrderDate"))
        .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    )


dp.create_streaming_table(
    name="silver.orders",
    comment="Orders fact-like table as SCD Type 1 (latest status/amount per OrderID).",
)


dp.create_auto_cdc_flow(
    target="silver.orders",
    source="orders_bronze_cleaned",
    keys=["OrderID"],
    sequence_by="ModifiedDate",
    stored_as_scd_type=1,
)
