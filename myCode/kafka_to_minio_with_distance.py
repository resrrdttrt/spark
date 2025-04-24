from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_date, from_unixtime, col, approx_count_distinct, lag, when, lit
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, DoubleType, ArrayType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import math

# Config variables
KAFKA_BOOTSTRAP_SERVERS = "172.21.6.68:9092"
KAFKA_TOPIC = "spark-consume"
MINIO_ENDPOINT = "http://172.21.6.68:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "vtrackingsparkwithdistance"
MINIO_PATH = "parquet"
MINIO_COUNT_PATH = "offline_entity_counts"
MINIO_DISTANCE_PATH = "distance_per_day"

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

# Define schema for the Kafka message payload
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

def write_offline_counts_to_parquet(df, batch_id):
    """Write offline entity counts to Parquet, overwriting the partition for each day."""
    df.write \
        .format("parquet") \
        .partitionBy("day") \
        .mode("overwrite") \
        .save(f"s3a://{MINIO_BUCKET}/{MINIO_COUNT_PATH}")

def process_distance_batch(df, batch_id):
    """Process each micro-batch to compute distances, print to console, and write to Parquet."""
    # Define window to get previous coordinates
    window_spec = Window.partitionBy("entity_id", "day").orderBy("ts")
    
    # Get previous latitude and longitude
    df = df.withColumn("prev_latitude", lag("latitude").over(window_spec)) \
           .withColumn("prev_longitude", lag("longitude").over(window_spec))
    
    # Compute distance between consecutive points
    df = df.withColumn("distance_km",
                       when((col("prev_latitude").isNotNull()) & (col("prev_longitude").isNotNull()),
                            haversine_udf(col("prev_latitude"), col("prev_longitude"), col("latitude"), col("longitude"))
                           ).otherwise(lit(0.0)))
    
    # Sum distances per entity_id and day
    distance_agg_df = df.groupBy("entity_id", "day") \
                        .agg(F.sum("distance_km").alias("total_distance_km"))
    
    # Print to console
    print(f"[INFO] Distance results for batch {batch_id}:")
    distance_agg_df.show(truncate=False)
    
    # Write to Parquet, overwriting partitions
    distance_agg_df.write \
        .format("parquet") \
        .partitionBy("entity_id", "day") \
        .mode("overwrite") \
        .save(f"s3a://{MINIO_BUCKET}/{MINIO_DISTANCE_PATH}")

def main():
    print("[INFO] Starting Spark job to consume from Kafka, count offline entities, calculate distances, and write to MinIO")
    print(f"[INFO] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Kafka topic: {KAFKA_TOPIC}")
    print(f"[INFO] MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"[INFO] MinIO bucket: {MINIO_BUCKET}")
    print(f"[INFO] MinIO path for raw data: {MINIO_PATH}")
    print(f"[INFO] MinIO path for offline counts: {MINIO_COUNT_PATH}")
    print(f"[INFO] MinIO path for distances: {MINIO_DISTANCE_PATH}")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("KafkaToMinIOConsume") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.apache.hadoop:hadoop-aws:3.3.6") \
        .getOrCreate()

    # Configure Hadoop settings using SparkContext
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
    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.sparkContext.setLogLevel("WARN")
    print("[INFO] Spark session created with Hadoop configurations.")

    # Test bucket existence
    print(f"[INFO] Testing if bucket exists: s3a://{MINIO_BUCKET}")
    try:
        bucket_path = f"s3a://{MINIO_BUCKET}/"
        files = spark.read.format("text").load(bucket_path).count()
        print(f"[INFO] Successfully connected to bucket. Found {files} files.")
    except Exception as e:
        print(f"[INFO] Error checking bucket: {str(e)}")
        print("[INFO] Ensure the bucket exists and is accessible. Continuing...")

    # Read from Kafka
    print("[INFO] Reading from Kafka...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    print("[INFO] Kafka DataFrame created.")

    value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")
    print("[INFO] Extracted value as string from Kafka DataFrame.")
    parsed_df = value_df.select(from_json("json_value", message_schema).alias("data")).select("data.*")
    print("[INFO] Parsed JSON messages with schema.")

    # Convert ts to day (assuming ts is in milliseconds)
    parsed_df = parsed_df.withColumn("day", to_date(from_unixtime(parsed_df.ts / 1000)))

    # Compute offline entity counts
    offline_df = parsed_df.filter(col("json_v.status") == "offline") \
                         .groupBy("day") \
                         .agg(approx_count_distinct("entity_id").alias("offline_entity_count"))

    # Prepare data for distance calculation
    distance_df = parsed_df.filter((col("json_v.latitude").isNotNull()) & (col("json_v.longitude").isNotNull())) \
                          .select("entity_id", "day", "ts", col("json_v.latitude").alias("latitude"), col("json_v.longitude").alias("longitude"))

    # Write offline entity counts to console
    print("[INFO] Writing offline entity counts to console...")
    console_offline_query = offline_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    # Write offline entity counts to MinIO
    print(f"[INFO] Writing offline entity counts to MinIO (s3a://{MINIO_BUCKET}/{MINIO_COUNT_PATH}), partitioned by day...")
    minio_offline_query = offline_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_offline_counts_to_parquet) \
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/offline_counts") \
        .start()

    # Process distances, print to console, and write to MinIO
    print(f"[INFO] Processing distances, printing to console, and writing to MinIO (s3a://{MINIO_BUCKET}/{MINIO_DISTANCE_PATH})...")
    minio_distance_query = distance_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_distance_batch) \
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/distances") \
        .start()

    # Existing write to MinIO for raw data
    print(f"[INFO] Writing raw data to MinIO (s3a://{MINIO_BUCKET}/{MINIO_PATH}), partitioned by entity_id and day...")
    raw_query = parsed_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"s3a://{MINIO_BUCKET}/{MINIO_PATH}") \
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/kafka") \
        .partitionBy("entity_id", "day") \
        .start()

    print("[INFO] Streaming queries started. Waiting for termination...")
    spark.streams.awaitAnyTermination()

    # Stop Spark session
    spark.stop()
    print("[INFO] Spark job completed")

if __name__ == "__main__":
    main()
