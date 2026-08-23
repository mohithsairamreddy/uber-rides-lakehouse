
from pyspark import pipelines as dp
from pyspark.sql import functions as F
 
@dp.materialized_view(name="uber_project.gold.dim_cancellation_reason")
def dim_cancellation_reason():
    return (
        spark.read.table("uber_project.bronze.map_cancellation_reasons_bronze")
        .select(F.col("cancellation_reason_id").cast("int").alias("cancellation_reason_id"),F.col("cancellation_reason").alias("cancellation_reason_name"),)
        .dropDuplicates(["cancellation_reason_id"])
    )