from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col

# Configuration Variables
KAFKA_BROKERS = "your_kafka_broker:9092"
KAFKA_TOPIC = "your_kafka_topic"
GCS_PATH = "gs://your_bucket/your_path"
CHECKPOINT_LOCATION = "gs://your_bucket/checkpoints"

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("KafkaToGCS") \
        .getOrCreate()

    # Define schema
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("code", StringType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("open", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("adjust", DoubleType(), True),
        StructField("volume_match", DoubleType(), True),
        StructField("value_match", DoubleType(), True)
    ])

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Convert value column to string and parse JSON
    json_df = df \
        .selectExpr("CAST(value AS STRING) as json") \
        .selectExpr(
            "from_json(json, '{}') as data".format(schema.simpleString())
        ) \
        .select("data.*")

    # Write to GCS
    json_df.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("path", GCS_PATH) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start() \
        .awaitTermination()

if __name__ == "__main__":
    main()