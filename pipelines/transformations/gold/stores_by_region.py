from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold.stores_by_region",
    comment="Number of stores by region and country."
)
def stores_by_region():
    return (
        spark.read.table("silver.stores")
        .groupBy("Region", "Country")
        .agg(F.count("*").alias("store_count"))
    )