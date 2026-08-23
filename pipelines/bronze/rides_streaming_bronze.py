from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

EH_NAMESPACE                    = spark.conf.get("uber_event_hub_namespace")
EH_NAME                         = spark.conf.get("uber_event_hub_names")

EH_CONN_STR = dbutils.secrets.get(catalog="uber_project", schema="bronze", key="uber_event_hub_connection_string")

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : spark.conf.get("uber_kafka_requestTimeout"),
  "kafka.session.timeout.ms" : spark.conf.get("uber_kafka_sessionTimeout"),
  "maxOffsetsPerTrigger"     : spark.conf.get("uber_kafka_maxOffsetsPerTrigger"),
  "failOnDataLoss"           : spark.conf.get("uber_kafka_failOnDataLoss"),
  "startingOffsets"          : spark.conf.get("uber_kafka_startingOffsets")
}

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

@dp.table
def rides_streaming_bronze():
    df = spark.readStream.format("kafka")\
                .options(**KAFKA_OPTIONS)\
                .load()

    df = df.select(
        col("key").cast("string").alias("event_key"),
        from_json(col("value").cast("string"), rides_schema).alias("ride"),col("topic"),col("partition"),col("offset"),col("timestamp").alias("kafka_timestamp")
    ).select("event_key", "ride.*", "topic", "partition", "offset", "kafka_timestamp")

    return df
