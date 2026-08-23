from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "uber_project.silver.silver_obt"

dp.create_streaming_table(name=TARGET,comment="Rides enriched with reference data (OBT)")

@dp.append_flow(target=TARGET, name="silver_obt_from_rides_staging")
def silver_obt_from_stg_rides():
    rides = spark.readStream.table("uber_project.bronze.rides_staging")

    statuses = spark.read.table("uber_project.bronze.map_ride_statuses_bronze")
    reasons = spark.read.table("uber_project.bronze.map_cancellation_reasons_bronze")
    payments = spark.read.table("uber_project.bronze.map_payment_methods_bronze")
    makes = spark.read.table("uber_project.bronze.map_vehicle_makes_bronze")
    types = spark.read.table("uber_project.bronze.map_vehicle_types_bronze")
    cities = spark.read.table("uber_project.bronze.map_cities_bronze")
    pickup_cities = cities.select(F.col("city_id").alias("pickup_city_id"),F.col("city").alias("pickup_city_name"))
    dropoff_cities = cities.select(F.col("city_id").alias("dropoff_city_id"),F.col("city").alias("dropoff_city_name"))

    return (
        rides
        .join(statuses, "ride_status_id", "left")
        .join(reasons, "cancellation_reason_id", "left")
        .join(payments, "payment_method_id", "left")
        .join(makes, "vehicle_make_id", "left")
        .join(types, "vehicle_type_id", "left")
        .join(pickup_cities, "pickup_city_id", "left")
        .join(dropoff_cities, "dropoff_city_id", "left")
    )