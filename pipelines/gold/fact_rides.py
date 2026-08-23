from pyspark import pipelines as dp
from pyspark.sql import functions as F
 
TARGET = "uber_project.gold.fact_rides"
COMPLETED_STATUSES = ["completed", "complete", "finished"]
CANCELLED_STATUSES = ["cancelled", "canceled"]
 
 
@dp.temporary_view(name="fact_rides_src")
def fact_rides_src():
    df = spark.readStream.table("uber_project.silver.silver_obt")
 
    ride_event_ts = F.coalesce(F.col("dropoff_timestamp"),F.col("pickup_timestamp"),F.col("booking_timestamp"),)
    status = F.lower(F.trim(F.col("ride_status")))
 
    return df.select(
        F.col("ride_id"),
        F.col("confirmation_number"),
        F.col("passenger_id"),
        F.col("driver_id"),
        F.col("vehicle_id"),
        F.col("vehicle_type_id"),
        F.col("vehicle_make_id"),
        F.col("payment_method_id"),
        F.col("ride_status_id"),
        F.col("cancellation_reason_id"),
        F.col("pickup_city_id"),
        F.col("dropoff_city_id"),
        F.date_format("booking_timestamp", "yyyyMMdd").cast("int").alias("booking_date_key"),
        F.date_format("pickup_timestamp", "yyyyMMdd").cast("int").alias("pickup_date_key"),
        F.col("ride_status").alias("ride_status_name"),
        F.col("payment_method").alias("payment_method_name"),
        F.col("cancellation_reason").alias("cancellation_reason_name"),
        F.col("vehicle_type"),
        F.col("vehicle_make"),
        F.col("pickup_city_name"),
        F.col("dropoff_city_name"),
        F.col("booking_timestamp"),
        F.col("pickup_timestamp"),
        F.col("dropoff_timestamp"),
        F.to_date("booking_timestamp").alias("booking_date"),
        F.hour("booking_timestamp").alias("booking_hour"),
        F.col("pickup_latitude"),
        F.col("pickup_longitude"),
        F.col("dropoff_latitude"),
        F.col("dropoff_longitude"),
        F.col("distance_miles"),
        F.col("duration_minutes"),
        F.col("base_fare"),
        F.col("distance_fare"),
        F.col("time_fare"),
        F.col("subtotal"),
        F.col("tip_amount"),
        F.col("total_fare"),
        F.col("surge_multiplier"),
        F.col("rating"),
        F.col("driver_rating"),
        F.round(
            (F.unix_timestamp("pickup_timestamp") - F.unix_timestamp("booking_timestamp")) / 60.0,
            2,
        ).alias("wait_minutes"),
        F.round(
            (F.unix_timestamp("dropoff_timestamp") - F.unix_timestamp("pickup_timestamp")) / 60.0,
            2,
        ).alias("actual_ride_minutes"),
        F.when(F.col("distance_miles") > 0, F.col("total_fare") / F.col("distance_miles"))
        .alias("fare_per_mile"),
        status.isin(COMPLETED_STATUSES).alias("is_completed"),
        status.isin(CANCELLED_STATUSES).alias("is_cancelled"),
        ride_event_ts.alias("ride_event_ts"),
    )
 
 
dp.create_streaming_table(
    name=TARGET,
    comment="Fact table at ride grain. One row per ride_id, latest state (SCD1).",
    table_properties={"delta.enableChangeDataFeed": "true"},
    cluster_by=["booking_date", "pickup_city_id"],
)
 
dp.create_auto_cdc_flow(
    target=TARGET,
    source="fact_rides_src",
    keys=["ride_id"],
    sequence_by="ride_event_ts",
    stored_as_scd_type="1",
)
 