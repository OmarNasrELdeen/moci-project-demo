from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="employees_bronze_cleaned",
    private=True,
    comment="Cleaned Bronze employees records used internally by the pipeline.",
)
def employees_bronze_cleaned():
    return (
        spark.readStream.table("bronze.employees")
        .where(F.col("EmployeeID").isNotNull())
        .where(F.col("StoreID").isNotNull())
        .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    )


dp.create_streaming_table(
    name="silver.employees",
    comment="Employees dimension as SCD Type 2 (job/store history retained).",
)


dp.create_auto_cdc_flow(
    target="silver.employees",
    source="employees_bronze_cleaned",
    keys=["EmployeeID"],
    sequence_by="ModifiedDate",
    stored_as_scd_type=2,
)
