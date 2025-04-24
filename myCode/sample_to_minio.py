from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import datetime
import random

# Config variables
# MINIO_ACCESS_KEY = "iot"
# MINIO_SECRET_KEY = "iot@2022"
# MINIO_ENDPOINT = "http://localhost:32642"
# MINIO_BUCKET = "test-bucket"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_ENDPOINT = "http://172.21.6.68:9000"
MINIO_BUCKET = "vehicle-data"


MINIO_PATH = "parquet"

def main():
    print("[INFO] Starting Spark job to write sample data to MinIO (parquet)")
    print(f"[INFO] MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"[INFO] MinIO bucket: {MINIO_BUCKET}")
    print(f"[INFO] MinIO path: {MINIO_PATH}")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("WriteToMinIOHadoopConfig") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
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

    spark.sparkContext.setLogLevel("WARN")
    print(f"[INFO] Created Spark session with version: {spark.version}")

    # Test bucket existence
    print(f"[INFO] Testing if bucket exists: s3a://{MINIO_BUCKET}")
    try:
        bucket_path = f"s3a://{MINIO_BUCKET}/"
        files = spark.read.format("text").load(bucket_path).count()
        print(f"[INFO] Successfully connected to bucket. Found {files} files.")
    except Exception as e:
        print(f"[INFO] Error checking bucket: {str(e)}")
        print("[INFO] Ensure the bucket exists and is accessible. Continuing...")

    # Generate sample vehicle data
    print("[INFO] Generating sample vehicle data...")
    vehicle_data = []
    vehicle_ids = ["V001", "V002", "V003", "V004", "V005"]
    statuses = ["running", "idle", "stopped", "maintenance"]

    # Generate 100 random records
    for i in range(100):
        vehicle_id = random.choice(vehicle_ids)
        status = random.choice(statuses)
        speed = random.randint(0, 120)
        fuel = random.uniform(0, 100)
        latitude = random.uniform(10.7, 10.9)
        longitude = random.uniform(106.4, 106.7)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        vehicle_data.append((vehicle_id, status, speed, fuel, latitude, longitude, timestamp))

    # Define schema
    schema = StructType([
        StructField("vehicle_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("speed", IntegerType(), False),
        StructField("fuel_level", DoubleType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("timestamp", StringType(), False)
    ])

    # Create DataFrame
    df = spark.createDataFrame(vehicle_data, schema)

    # Show sample data
    print("[INFO] Sample data:")
    df.show(5, truncate=False)

    # Write to MinIO
    print(f"[INFO] Writing data to MinIO s3a://{MINIO_BUCKET}/{MINIO_PATH}")
    try:
        df.write \
            .mode("overwrite") \
            .partitionBy("vehicle_id") \
            .parquet(f"s3a://{MINIO_BUCKET}/{MINIO_PATH}")
        print("[INFO] Successfully wrote data to MinIO")
    except Exception as e:
        print(f"[ERROR] Failed to write to MinIO: {str(e)}")

    # Stop Spark session
    spark.stop()
    print("[INFO] Spark job completed")

if __name__ == "__main__":
    main()
