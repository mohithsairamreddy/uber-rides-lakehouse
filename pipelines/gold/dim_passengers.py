
from pyspark import pipelines as dp
from pyspark.sql import functions as F
 
TARGET = "uber_project.gold.dim_passenger"
 
 
@dp.temporary_view(name="dim_passenger_src")
def dim_passenger_src():
    return (
        spark.readStream.table("uber_project.silver.silver_obt")
        .where(F.col("passenger_id").isNotNull())
        .select("passenger_id","passenger_name","passenger_email","passenger_phone",F.col("booking_timestamp").alias("effective_ts"),)
    )
 
 
dp.create_streaming_table(name=TARGET)
 
dp.create_auto_cdc_flow(
    target=TARGET,
    source="dim_passenger_src",
    keys=["passenger_id"],
    sequence_by="effective_ts",
    stored_as_scd_type="2",
    track_history_except_column_list=["effective_ts"]
)
 