from pyspark import pipelines as dp
from pyspark.sql import functions as F
 
TARGET = "uber_project.gold.dim_driver"
 
 
@dp.temporary_view(name="dim_driver_src")
def dim_driver_src():
    return (
        spark.readStream.table("uber_project.silver.silver_obt")
        .where(F.col("driver_id").isNotNull())
        .select("driver_id","driver_name","driver_phone","driver_license","driver_rating",F.col("booking_timestamp").alias("effective_ts"),)
        )
 
 
dp.create_streaming_table(name=TARGET)
 
dp.create_auto_cdc_flow(
    target=TARGET,
    source="dim_driver_src",
    keys=["driver_id"],
    sequence_by="effective_ts",
    stored_as_scd_type="2",
    track_history_except_column_list=["driver_rating", "effective_ts"]
)