
from pyspark import pipelines as dp
from pyspark.sql import functions as F
 
@dp.materialized_view(name="uber_project.gold.dim_ride_status")
def dim_ride_status():
    return (
        spark.read.table("uber_project.bronze.map_ride_statuses_bronze")
        .select(F.col("ride_status_id").cast("int").alias("ride_status_id"),F.col("ride_status").alias("ride_status_name"),)
        .dropDuplicates(["ride_status_id"])
    )
 