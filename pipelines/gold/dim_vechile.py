from pyspark import pipelines as dp
from pyspark.sql import functions as F
 
TARGET = "uber_project.gold.dim_vehicle"
 
 
@dp.temporary_view(name="dim_vehicle_src")
def dim_vehicle_src():
    return (
        spark.readStream.table("uber_project.silver.silver_obt").where(F.col("vehicle_id").isNotNull())
        .select("vehicle_id","vehicle_type_id","vehicle_type","vehicle_make_id","vehicle_make","vehicle_model","vehicle_color","license_plate",
            F.col("booking_timestamp").alias("effective_ts"),
        )
    )
 
 
dp.create_streaming_table(name=TARGET)
 
dp.create_auto_cdc_flow(
    target=TARGET,
    source="dim_vehicle_src",
    keys=["vehicle_id"],
    sequence_by="effective_ts",
    stored_as_scd_type="2",
    track_history_except_column_list=["effective_ts"]
)