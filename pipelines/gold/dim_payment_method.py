from pyspark import pipelines as dp
from pyspark.sql import functions as F
 
@dp.materialized_view(name="uber_project.gold.dim_payment_method")
def dim_payment_method():
    return (
        spark.read.table("uber_project.bronze.map_payment_methods_bronze")
        .select(F.col("payment_method_id").cast("int").alias("payment_method_id"),F.col("payment_method").alias("payment_method_name"),)
        .dropDuplicates(["payment_method_id"])
    )
 