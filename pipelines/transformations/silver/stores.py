from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ------------------------------------------------------------------
# Bronze Cleanup (Private Intermediate Table)
# ------------------------------------------------------------------
@dp.table(
    name="stores_bronze_cleaned",
    private=True,
    comment="Cleaned bronze records used internally by the pipeline."
)
def stores_bronze_cleaned():
    return (
        spark.readStream.table("bronze.stores")
        .where(F.col("StoreID").isNotNull())
        .where(F.col("StoreCode").isNotNull())
        .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    )


# ------------------------------------------------------------------
# Create Silver Target Table
# ------------------------------------------------------------------
dp.create_streaming_table(
    name="silver.stores",
    comment="Current version of stores (SCD Type 1)"
)


# ------------------------------------------------------------------
# Apply CDC (Upsert)
# ------------------------------------------------------------------
dp.create_auto_cdc_flow(
    target="silver.stores",
    source="stores_bronze_cleaned",
    keys=["StoreID"],
    sequence_by="ModifiedDate",
    stored_as_scd_type=1
)