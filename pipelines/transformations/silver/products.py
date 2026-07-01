from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="products_bronze_cleaned",
    private=True,
    comment="Cleaned Bronze products records used internally by the pipeline.",
)
def products_bronze_cleaned():
    return (
        spark.readStream.table("bronze.products")
        .where(F.col("ProductID").isNotNull())
        .where(F.col("ProductCode").isNotNull())
        .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    )


dp.create_streaming_table(
    name="silver.products",
    comment="Products dimension as SCD Type 2 (history retained for changes).",
)


dp.create_auto_cdc_flow(
    target="silver.products",
    source="products_bronze_cleaned",
    keys=["ProductID"],
    sequence_by="ModifiedDate",
    stored_as_scd_type=2,
)
