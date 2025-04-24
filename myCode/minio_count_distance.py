from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, lit
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, DoubleType, ArrayType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import math
from datetime import datetime

# Config variables
MINIO_ENDPOINT = "http://172.21.6.68:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "vtrackingsparkwithdistance"
MINIO_RAW_PATH = "parquet"
MINIO_OUTPUT_PATH = "historical_distances"

# Earth's radius in kilometers
EARTH_RADIUS = 6371.0

# Define Haversine UDF
def haversine(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Differences
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    # Haversine formula
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = EARTH_RADIUS * c
    return distance

# Register UDF
haversine_udf = F.udf(haversine, DoubleType())

# Define schema (same as streaming job)
json_v_schema = StructType([
    StructField("direction", LongType(), True),
    StructField("geocoding", StringType(), True),
    StructField("history", BooleanType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("odometer", DoubleType(), True),
    StructField("speed", LongType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", LongType(), True)
])

attribute_sub_schema = StructType([
    StructField("entity_type", StringType(), True),
    StructField("entity_id", StringType(), True),
    StructField("attribute_type", StringType(), True),
    StructField("attribute_key", StringType(), True),
    StructField("logged", BooleanType(), True),
    StructField("bool_v", BooleanType(), True),
    StructField("str_v", StringType(), True),
    StructField("long_v", LongType(), True),
    StructField("dbl_v", DoubleType(), True),
    StructField("json_v", json_v_schema, True),
    StructField("last_update_ts", LongType(), True),
    StructField("ts", LongType(), True),
    StructField("value_type", StringType(), True),
    StructField("value_nil", BooleanType(), True),
    StructField("new_attribute_key", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("not_send_ws", BooleanType(), True),
    StructField("AttributeSub", ArrayType(StringType()), True)
])

message_schema = StructType([
    StructField("entity_type", StringType(), True),
    StructField("entity_id", StringType(), True),
    StructField("attribute_type", StringType(), True),
    StructField("attribute_key", StringType(), True),
    StructField("logged", BooleanType(), True),
    StructField("bool_v", BooleanType(), True),
    StructField("str_v", StringType(), True),
    StructField("long_v", LongType(), True),
    StructField("dbl_v", DoubleType(), True),
    StructField("json_v", json_v_schema, True),
    StructField("last_update_ts", LongType(), True),
    StructField("ts", LongType(), True),
    StructField("value_type", StringType(), True),
    StructField("value_nil", BooleanType(), True),
    StructField("new_attribute_key", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("not_send_ws", BooleanType(), True),
    StructField("AttributeSub", ArrayType(attribute_sub_schema), True)
])

def compute_historical_distances(start_date: str, end_date: str):
    """
    Compute total distance per day for each entity_id from start_date to end_date.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format (e.g., '2025-04-01')
        end_date (str): End date in YYYY-MM-DD format (e.g., '2025-04-30')
    """
    print(f"[INFO] Starting batch job to compute historical distances from {start_date} to {end_date}")
    print(f"[INFO] Reading raw data from s3a://{MINIO_BUCKET}/{MINIO_RAW_PATH}")
    print(f"[INFO] Writing results to s3a://{MINIO_BUCKET}/{MINIO_OUTPUT_PATH}")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("HistoricalDistanceCalculation") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.6") \
        .getOrCreate()

    # Configure Hadoop settings for MinIO
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", MINIO_ENDPOINT)
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    sc._jsc.hadoopConfiguration().set("fs.s3a.multipart.enabled", "false")
    sc._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")

    # Suppress warnings
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.sparkContext.setLogLevel("WARN")
    print("[INFO] Spark session created with Hadoop configurations.")

    # Read raw data from MinIO
    try:
        raw_df = spark.read.parquet(f"s3a://{MINIO_BUCKET}/{MINIO_RAW_PATH}")
        print("[INFO] Successfully read raw data from MinIO.")
    except Exception as e:
        print(f"[ERROR] Failed to read data from s3a://{MINIO_BUCKET}/{MINIO_RAW_PATH}: {str(e)}")
        spark.stop()
        return

    # Filter data for the specified date range
    distance_df = raw_df.filter((col("day").between(start_date, end_date)) &
                               (col("json_v.latitude").isNotNull()) &
                               (col("json_v.longitude").isNotNull())) \
                       .select("entity_id", "day", "ts",
                               col("json_v.latitude").alias("latitude"),
                               col("json_v.longitude").alias("longitude"))

    # Define window to get previous coordinates
    window_spec = Window.partitionBy("entity_id").orderBy("ts")

    # Get previous latitude and longitude
    distance_df = distance_df.withColumn("prev_latitude", lag("latitude").over(window_spec)) \
                            .withColumn("prev_longitude", lag("longitude").over(window_spec))

    # Compute distance between consecutive points
    distance_df = distance_df.withColumn("distance_km",
                                        when((col("prev_latitude").isNotNull()) & (col("prev_longitude").isNotNull()),
                                             haversine_udf(col("prev_latitude"), col("prev_longitude"), col("latitude"), col("longitude"))
                                            ).otherwise(lit(0.0)))

    # Sum distances per entity_id and day
    result_df = distance_df.groupBy("entity_id", "day") \
                          .agg(F.sum("distance_km").alias("total_distance_km"))

    # Show results in console
    print(f"[INFO] Total distance per entity_id and day from {start_date} to {end_date}:")
    result_df.orderBy("day", "entity_id").show(truncate=False)

    # Write results to MinIO
    print(f"[INFO] Writing results to s3a://{MINIO_BUCKET}/{MINIO_OUTPUT_PATH}")
    try:
        result_df.write \
            .format("parquet") \
            .partitionBy("entity_id", "day") \
            .mode("overwrite") \
            .save(f"s3a://{MINIO_BUCKET}/{MINIO_OUTPUT_PATH}")
        print("[INFO] Successfully wrote results to MinIO.")
    except Exception as e:
        print(f"[ERROR] Failed to write results to s3a://{MINIO_BUCKET}/{MINIO_OUTPUT_PATH}: {str(e)}")

    # Stop Spark session
    spark.stop()
    print("[INFO] Batch job completed.")

if __name__ == "__main__":
    # Example date range (replace with your desired dates)
    start_date = "2025-04-01"
    end_date = "2025-04-30"
    compute_historical_distances(start_date, end_date)