from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, avg

# ----------------------------
# PostgreSQL Config
# ----------------------------
POSTGRES_URL = "jdbc:postgresql://postgres:5432/telemetrydb"
POSTGRES_PROPERTIES = {
    "user": "telemetry",
    "password": "telemetry",
    "driver": "org.postgresql.Driver"
}

# ----------------------------
# Spark Session
# ----------------------------
spark = SparkSession.builder \
    .appName("Telemetry Analytics Job") \
    .getOrCreate()

# ----------------------------
# Read enriched telemetry
# ----------------------------
telemetry_df = spark.read.jdbc(
    url=POSTGRES_URL,
    table="enriched_events",
    properties=POSTGRES_PROPERTIES
)

# ----------------------------
# Device Daily Stats
# ----------------------------
device_daily_df = telemetry_df \
    .withColumn("event_date", to_date(col("event_time"))) \
    .groupBy("device_id", "event_date") \
    .agg(
        count("*").alias("total_events"),
        avg("battery_level").alias("avg_battery_level")
    )

device_daily_df.write.jdbc(
    url=POSTGRES_URL,
    table="device_daily_stats",
    mode="overwrite",   # safe for analytics tables
    properties=POSTGRES_PROPERTIES
)

# ----------------------------
# App Version Usage
# ----------------------------
app_version_df = telemetry_df \
    .withColumn("event_date", to_date(col("event_time"))) \
    .groupBy("app_version", "event_date") \
    .agg(count("*").alias("event_count"))

app_version_df.write.jdbc(
    url=POSTGRES_URL,
    table="app_version_usage",
    mode="overwrite",
    properties=POSTGRES_PROPERTIES
)

# ----------------------------
# Stop Spark
# ----------------------------
spark.stop()
print("Telemetry analytics job completed successfully!")
