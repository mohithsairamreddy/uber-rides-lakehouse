from pyspark import pipelines as dp

common_cols = [
    "ride_id", "confirmation_number", "passenger_id", "driver_id", "vehicle_id",
    "pickup_location_id", "dropoff_location_id", "vehicle_type_id", "vehicle_make_id",
    "payment_method_id", "ride_status_id", "pickup_city_id", "dropoff_city_id",
    "cancellation_reason_id", "passenger_name", "passenger_email", "passenger_phone",
    "driver_name", "driver_rating", "driver_phone", "driver_license", "vehicle_model",
    "vehicle_color", "license_plate", "pickup_address", "pickup_latitude", "pickup_longitude",
    "dropoff_address", "dropoff_latitude", "dropoff_longitude", "distance_miles",
    "duration_minutes", "booking_timestamp", "pickup_timestamp", "dropoff_timestamp",
    "base_fare", "distance_fare", "time_fare", "surge_multiplier", "subtotal",
    "tip_amount", "total_fare", "rating"
]

dp.create_streaming_table("rides_staging")

@dp.append_flow(target="rides_staging", once=True)
def bulk_rides_flow():
    return spark.read.table("uber_project.bronze.bulk_rides_bronze").select(*common_cols)

@dp.append_flow(target="rides_staging")
def streaming_rides_flow():
    return spark.readStream.table("uber_project.bronze.rides_streaming_bronze").select(*common_cols)