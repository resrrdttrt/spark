from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_date, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, DoubleType

# Config variables
KAFKA_BOOTSTRAP_SERVERS = "172.21.6.68:9092"
KAFKA_TOPIC = "sparkts"
MINIO_ENDPOINT = "http://172.21.6.68:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "vtrackingts"
MINIO_PATH = "json"

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

def main():
    print("[INFO] Starting Spark job to consume from Kafka and write to MinIO (JSON format, partitioned by id and day)")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("KafkaToMinIOConsume") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.apache.hadoop:hadoop-aws:3.3.6") \
        .getOrCreate()

    # Set Hadoop configurations for MinIO
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    spark.sparkContext.setLogLevel("WARN")
    print("[INFO] Spark session initialized.")

    # Read Kafka stream
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    print("[INFO] Connected to Kafka topic.")

    # Parse Kafka JSON value
    value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")
    parsed_df = value_df.select(from_json("json_value", message_schema).alias("data")).select("data.*")

    # Add `day` column from timestamp
    parsed_df = parsed_df.withColumn("day", to_date(from_unixtime(parsed_df.ts / 1000)))

    # Write stream to MinIO
    print(f"[INFO] Writing to MinIO bucket: s3a://{MINIO_BUCKET}/{MINIO_PATH}")
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", f"s3a://{MINIO_BUCKET}/{MINIO_PATH}") \
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/kafka") \
        .partitionBy("id", "day") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
