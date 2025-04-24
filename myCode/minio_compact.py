from pyspark.sql import SparkSession
import datetime
import uuid

# Config variables
MINIO_ENDPOINT = "http://172.21.6.68:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "vtrackingsparkwithdistance"
MINIO_PATH = "parquet"
COMPACT_TYPE = "zstd" # snappy, gzip


def get_partitions(spark, base_path):
    """Retrieve all entity_id and day partitions from the MinIO directory structure, excluding the current day."""
    try:
        current_day = datetime.datetime.now().strftime("%Y-%m-%d")
        # Read Parquet files with partition discovery
        df = spark.read.option("basePath", base_path).parquet(f"{base_path}/*/*")
        # Infer entity_id and day from partition columns
        df = df.select("entity_id", "day").distinct()
        # Filter out the current day
        df = df.filter(df.day != current_day)
        partitions = [(row.entity_id, row.day) for row in df.collect()]
        print(f"[INFO] Found {len(partitions)} partitions: {partitions}")
        return partitions
    except Exception as e:
        print(f"[ERROR] Failed to retrieve partitions: {str(e)}")
        return []

def is_valid_partition(spark, input_path):
    """Check if a partition contains valid Parquet files."""
    try:
        df = spark.read.parquet(input_path)
        return df.count() > 0  # Ensure there’s at least one row
    except Exception as e:
        print(f"[WARN] Invalid or empty partition {input_path}: {str(e)}")
        return False

def compact_partition(spark, entity_id, day):
    """Compact a single partition and append to a new location, then provide commands to delete original data."""
    input_path = f"s3a://{MINIO_BUCKET}/{MINIO_PATH}/entity_id={entity_id}/day={day}"
    compact_path = f"s3a://{MINIO_BUCKET}/compress_{COMPACT_TYPE}/entity_id={entity_id}/day={day}"

    print(f"[INFO] Checking partition: {input_path}")
    if not is_valid_partition(spark, input_path):
        print(f"[INFO] Skipping compaction for invalid or empty partition: {input_path}")
        return

    print(f"[INFO] Compacting partition: {input_path}")
    try:
        # Read all Parquet files in the partition
        df = spark.read.parquet(input_path)
        print(f"[INFO] Input path is {input_path}")
        # Repartition to reduce file count (e.g., 1 file)
        df = df.coalesce(1)
        # Write compacted data to new compact location in append mode
        #df.write \
        #    .mode("append") \
        #    .parquet(compact_path)
        df.write \
            .mode("append") \
            .option("compression",COMPACT_TYPE) \
            .parquet(compact_path)
        print(f"[INFO] Successfully appended compacted data to new location: {compact_path}")
        print(f"[INFO] To delete original data, manually run:")
        print(f"  mc rm --recursive --force s3/{MINIO_BUCKET}/{MINIO_PATH}/entity_id={entity_id}/day={day}/*.parquet")
    except Exception as e:
        print(f"[ERROR] Failed to compact partition {input_path}: {str(e)}")

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MinIOParquetCompactionAllPreviousDays") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.6") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
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

    spark.sparkContext.setLogLevel("WARN")
    print("[INFO] Spark session created with Hadoop configurations.")

    # Get all partitions (entity_id and day, excluding current day)
    base_path = f"s3a://{MINIO_BUCKET}/{MINIO_PATH}"
    partitions = get_partitions(spark, base_path)

    if not partitions:
        print("[WARN] No partitions found for previous days. Exiting compaction job.")
        spark.stop()
        return

    # Compact each partition
    for entity_id, day in partitions:
        compact_partition(spark, entity_id, day)

    # Stop Spark session
    spark.stop()
    print("[INFO] Compaction job completed")

if __name__ == "__main__":
    main()
