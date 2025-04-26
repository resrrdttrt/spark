from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_date, from_unixtime, col, approx_count_distinct
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, DoubleType

# Config variables
KAFKA_BOOTSTRAP_SERVERS = "172.21.6.68:9092"
KAFKA_TOPIC = "sparkts"
MINIO_ENDPOINT = "http://172.21.6.68:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "vtrackingsparkwithcountoffline"
MINIO_PATH = "parquet"
MINIO_COUNT_PATH = "offline_entity_counts"

# Define schema for the Kafka message payload
extrainfo_schema = StructType([
    StructField("aircon", BooleanType(), True),
    StructField("door", BooleanType(), True),
    StructField("driverLicense", StringType(), True),
    StructField("driverName", StringType(), True),
    StructField("ignition", BooleanType(), True),
    StructField("ts", LongType(), True)
])

message_schema = StructType([
    StructField("id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("speed", LongType(), True),
    StructField("direction", LongType(), True),
    StructField("geocoding", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("ts", LongType(), True),
    StructField("plate_no", StringType(), True),
    StructField("driver_name", StringType(), True),
    StructField("driver_license", StringType(), True),
    StructField("history", BooleanType(), True),
    StructField("extrainfo", extrainfo_schema, True)
])

def write_batch_to_parquet(df, batch_id):
    """Write each micro-batch to Parquet, overwriting the partition for each day."""
    df.write \
        .format("parquet") \
        .partitionBy("day") \
        .mode("overwrite") \
        .save(f"s3a://{MINIO_BUCKET}/{MINIO_COUNT_PATH}")

def main():
    print("[INFO] Starting Spark job to consume from Kafka, count offline entities, and write to MinIO")
    print(f"[INFO] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Kafka topic: {KAFKA_TOPIC}")
    print(f"[INFO] MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"[INFO] MinIO bucket: {MINIO_BUCKET}")
    print(f"[INFO] MinIO path for raw data: {MINIO_PATH}")
    print(f"[INFO] MinIO path for offline counts: {MINIO_COUNT_PATH}")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("KafkaToMinIOConsumeWithCountOffline") \
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
    sc._jsc.hadoopConfiguration().set("fs.3a.multipart.enabled", "false")
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

    # Filter for offline status and compute offline entity counts
    offline_df = parsed_df.filter(col("status") == "offline") \
                         .groupBy("day") \
                         .agg(approx_count_distinct("id").alias("offline_entity_count"))

    # Write offline entity counts to console for display
    print("[INFO] Writing offline entity counts to console...")
    console_query = offline_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    # Write offline entity counts to MinIO in Parquet format using foreachBatch
    print(f"[INFO] Writing offline entity counts to MinIO (s3a://{MINIO_BUCKET}/{MINIO_COUNT_PATH}), partitioned by day...")
    minio_query = offline_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_batch_to_parquet) \
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/offline_counts") \
        .start()

    # Write raw data to MinIO
    print(f"[INFO] Writing raw data to MinIO (s3a://{MINIO_BUCKET}/{MINIO_PATH}), partitioned by id and day...")
    raw_query = parsed_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"s3a://{MINIO_BUCKET}/{MINIO_PATH}") \
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/kafka") \
        .partitionBy("id", "day") \
        .start()

    print("[INFO] Streaming queries started. Waiting for termination...")
    spark.streams.awaitAnyTermination()

    # Stop Spark session
    spark.stop()
    print("[INFO] Spark job completed")

if __name__ == "__main__":
    main()