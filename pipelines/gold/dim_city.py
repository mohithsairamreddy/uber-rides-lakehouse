from pyspark import pipelines as dp
from pyspark.sql import functions as F
 
 
@dp.materialized_view(name="uber_project.gold.dim_city",comment="City reference data, used for both pickup and dropoff roles.",)
def dim_city():
    return (
        spark.read.table("uber_project.bronze.map_cities_bronze")
        .select(F.col("city_id").cast("int").alias("city_id"),F.col("city").alias("city_name"),)
        .dropDuplicates(["city_id"])  
    )