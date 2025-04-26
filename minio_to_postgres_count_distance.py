from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, lit, row_number
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, DoubleType, ArrayType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import math
from datetime import datetime

# Config variables for MinIO (input)
MINIO_ENDPOINT = "http://172.21.6.68:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "vtrackingsparkwithdistance"
MINIO_RAW_PATH = "parquet"

# Config variables for PostgreSQL (output)
POSTGRES_HOST = "172.21.6.67"
POSTGRES_PORT = "5432"
POSTGRES_DB = "attributes"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "newpassword"
POSTGRES_TABLE = "historical_distances"
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Earth's radius in kilometers
EARTH_RADIUS = 6371.0

# Define Haversine UDF
def haversine(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
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
    Args:
        start_date (str): Start date in YYYY-MM-DD format (e.g., '2025-04-01')
        end_date (str): End date in YYYY-MM-DD format (e.g., '2025-04-30')
    """
    print(f"[INFO] Starting batch job to compute historical distances from {start_date} to {end_date}")
    print(f"[INFO] Reading raw data from s3a://{MINIO_BUCKET}/{MINIO_RAW_PATH}")
    print(f"[INFO] Writing results to PostgreSQL table {POSTGRES_TABLE}")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("HistoricalDistanceCalculation") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.6,org.postgresql:postgresql:42.7.3") \
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
    print("[INFO] Spark session created with Hadoop and JDBC configurations.")

    # Read raw data from MinIO
    try:
        raw_df = spark.read.parquet(f"s3a://{MINIO_BUCKET}/{MINIO_RAW_PATH}")
        print("[INFO] Successfully read raw data from MinIO.")
    except Exception as e:
        print(f"[ERROR] Failed to read data from s3a://{MINIO_BUCKET}/{MINIO_RAW_PATH}: {str(e)}")
        spark.stop()
        return

    # Filter data for the specified date range and valid coordinates
    distance_df = raw_df.filter((col("day").between(start_date, end_date)) &
                               (col("json_v.latitude").isNotNull()) &
                               (col("json_v.longitude").isNotNull()) &
                               (~col("json_v.status").isin("badgps", "offline"))) \
                       .select("entity_id", "day", "ts",
                               col("json_v.latitude").alias("latitude"),
                               col("json_v.longitude").alias("longitude"),
                               col("json_v.status").alias("status"),
                               col("json_v.speed").cast("double").alias("speed"))

    # Define window to identify first valid record and get previous values
    window_spec = Window.partitionBy("entity_id", "day").orderBy("ts")
    # Window for metrics across all records
    metrics_window = Window.partitionBy("entity_id", "day")

    # Assign row numbers to find the first valid record
    distance_df = distance_df.withColumn("row_num", row_number().over(window_spec))

    # Get previous latitude, longitude, status, and ts
    distance_df = distance_df.withColumn("prev_latitude", lag("latitude").over(window_spec)) \
                            .withColumn("prev_longitude", lag("longitude").over(window_spec)) \
                            .withColumn("prev_status", lag("status").over(window_spec)) \
                            .withColumn("prev_ts", lag("ts").over(window_spec))

    # Compute distance with Go logic
    distance_df = distance_df.withColumn("distance_km",
        when(
            # Skip first row (row_num == 1) or null previous values
            (col("row_num") == 1) | 
            (col("prev_latitude").isNull()) | 
            (col("prev_longitude").isNull()) | 
            (col("prev_ts").isNull()),
            lit(0.0)
        ).when(
            # Skip if current and previous status are same and both are stop/park
            (col("status") == col("prev_status")) & 
            (col("status").isin("stop", "park")) & 
            (col("status") == lag("status", 1).over(window_spec)),
            lit(0.0)
        ).when(
            # Include distance if coordinates are close
            (F.abs(col("latitude") - col("prev_latitude")) <= 0.1) & 
            (F.abs(col("longitude") - col("prev_longitude")) <= 0.1),
            haversine_udf(col("prev_latitude"), col("prev_longitude"), col("latitude"), col("longitude"))
        ).when(
            # Include distance if implied speed <= 150 km/h
            (col("ts") - col("prev_ts") != 0) & 
            (haversine_udf(col("prev_latitude"), col("prev_longitude"), col("latitude"), col("longitude")) * 3600000 / (col("ts") - col("prev_ts")) <= 150),
            haversine_udf(col("prev_latitude"), col("prev_longitude"), col("latitude"), col("longitude"))
        ).otherwise(lit(0.0))
    )

    # Compute metrics
    metrics_df = distance_df.groupBy("entity_id", "day").agg(
        F.sum("distance_km").alias("total_distance_km"),
        F.max("speed").alias("max_speed"),
        F.avg(when(col("speed") > 0, col("speed")).otherwise(None)).alias("average_speed"),
        F.sum(when(col("speed") > 0, 1).otherwise(0)).cast("long").alias("num_of_record")
    )

    # Show results in console
    print(f"[INFO] Total distance and metrics per entity_id and day from {start_date} to {end_date}:")
    metrics_df.orderBy("day", "entity_id").show(truncate=False)

    # Write results to PostgreSQL
    print(f"[INFO] Writing results to PostgreSQL table {POSTGRES_TABLE}")
    try:
        metrics_df.write \
            .format("jdbc") \
            .option("url", POSTGRES_JDBC_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("[INFO] Successfully wrote results to PostgreSQL.")
    except Exception as e:
        print(f"[ERROR] Failed to write results to PostgreSQL table {POSTGRES_TABLE}: {str(e)}")

    # Stop Spark session
    spark.stop()
    print("[INFO] Batch job completed.")

if __name__ == "__main__":
    # Example date range
    start_date = "2025-04-01"
    end_date = "2025-04-30"
    compute_historical_distances(start_date, end_date)