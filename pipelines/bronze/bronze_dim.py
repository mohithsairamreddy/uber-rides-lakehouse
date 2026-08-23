from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

BASE_PATH = "abfss://raw@uberprojectadls.dfs.core.windows.net"

rides_schema = StructType([
    StructField("ride_id", StringType()),
    StructField("confirmation_number", StringType()),
    StructField("passenger_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("pickup_location_id", StringType()),
    StructField("dropoff_location_id", StringType()),
    StructField("vehicle_type_id", IntegerType()),
    StructField("vehicle_make_id", IntegerType()),
    StructField("payment_method_id", IntegerType()),
    StructField("ride_status_id", IntegerType()),
    StructField("pickup_city_id", IntegerType()),
    StructField("dropoff_city_id", IntegerType()),
    StructField("cancellation_reason_id", IntegerType()),
    StructField("passenger_name", StringType()),
    StructField("passenger_email", StringType()),
    StructField("passenger_phone", StringType()),
    StructField("driver_name", StringType()),
    StructField("driver_rating", DoubleType()),
    StructField("driver_phone", StringType()),
    StructField("driver_license", StringType()),
    StructField("vehicle_model", StringType()),
    StructField("vehicle_color", StringType()),
    StructField("license_plate", StringType()),
    StructField("pickup_address", StringType()),
    StructField("pickup_latitude", DoubleType()),
    StructField("pickup_longitude", DoubleType()),
    StructField("dropoff_address", StringType()),
    StructField("dropoff_latitude", DoubleType()),
    StructField("dropoff_longitude", DoubleType()),
    StructField("distance_miles", DoubleType()),
    StructField("duration_minutes", IntegerType()),
    StructField("booking_timestamp", TimestampType()),
    StructField("pickup_timestamp", TimestampType()),
    StructField("dropoff_timestamp", TimestampType()),
    StructField("base_fare", DoubleType()),
    StructField("distance_fare", DoubleType()),
    StructField("time_fare", DoubleType()),
    StructField("surge_multiplier", DoubleType()),
    StructField("subtotal", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("total_fare", DoubleType()),
    StructField("rating", DoubleType())
])

# Map/reference files — schema inference is fine, they're small and simple
map_files = [
    "map_cities", "map_cancellation_reasons", "map_payment_methods",
    "map_ride_statuses", "map_vehicle_makes", "map_vehicle_types"
]

def make_map_table(file_name):
    @dp.table(name=f"{file_name}_bronze")
    def _map_bronze():
        return (
            spark.read
            .format("json")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .load(f"{BASE_PATH}/landing/{file_name}.json")
        )
    return _map_bronze

for file_name in map_files:
    make_map_table(file_name)

# bulk_rides — explicit schema so types match the streaming side exactly
@dp.table(name="bulk_rides_bronze")
def bulk_rides_bronze():
    return (
        spark.read
        .format("json")
        .schema(rides_schema)
        .option("multiLine", "true")
        .load(f"{BASE_PATH}/landing/bulk_rides.json")
    )