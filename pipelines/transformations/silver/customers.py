from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="customers_bronze_cleaned",
    private=True,
    comment="Cleaned Bronze customers records used internally by the pipeline.",
)
def customers_bronze_cleaned():
    return (
        spark.readStream.table("bronze.customers")
        .where(F.col("CustomerID").isNotNull())
        .where(F.col("Email").isNotNull())
        .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    )


dp.create_streaming_table(
    name="silver.customers",
    comment="Customers dimension as SCD Type 1 (latest values win).",
)


dp.create_auto_cdc_flow(
    target="silver.customers",
    source="customers_bronze_cleaned",
    keys=["CustomerID"],
    sequence_by="ModifiedDate",
    stored_as_scd_type=1,
)
