from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import *

# Config variables
KAFKA_BOOTSTRAP_SERVERS = "172.21.6.68:9092"
KAFKA_TOPIC = "spark-consume"

# Define schema for the Kafka message payload
attribute_sub_schema = StructType([
    StructField("entity_type", StringType()),
    StructField("entity_id", StringType()),
    StructField("attribute_type", StringType()),
    StructField("attribute_key", StringType()),
    StructField("logged", BooleanType()),
    StructField("bool_v", BooleanType()),
    StructField("str_v", StringType()),
    StructField("long_v", LongType()),
    StructField("dbl_v", DoubleType()),
    StructField("json_v", MapType(StringType(), StringType()), True),
    StructField("last_update_ts", LongType()),
    StructField("ts", LongType()),
    StructField("value_type", StringType()),
    StructField("value_nil", BooleanType()),
    StructField("new_attribute_key", StringType()),
    StructField("project_id", StringType()),
    StructField("not_send_ws", BooleanType()),
    StructField("AttributeSub", ArrayType(StringType()), True)
])

message_schema = StructType([
    StructField("entity_type", StringType()),
    StructField("entity_id", StringType()),
    StructField("attribute_type", StringType()),
    StructField("attribute_key", StringType()),
    StructField("logged", BooleanType()),
    StructField("bool_v", BooleanType()),
    StructField("str_v", StringType()),
    StructField("long_v", LongType()),
    StructField("dbl_v", DoubleType()),
    StructField("json_v", MapType(StringType(), StringType()), True),
    StructField("last_update_ts", LongType()),
    StructField("ts", LongType()),
    StructField("value_type", StringType()),
    StructField("value_nil", BooleanType()),
    StructField("new_attribute_key", StringType()),
    StructField("project_id", StringType()),
    StructField("not_send_ws", BooleanType()),
    StructField("AttributeSub", ArrayType(attribute_sub_schema), True)
])

def main():
    print("[INFO] Starting Spark job to consume from Kafka and print to terminal.")
    print(f"[INFO] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Kafka topic: {KAFKA_TOPIC}")

    spark = SparkSession.builder \
        .appName("KafkaToConsole") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("[INFO] Spark session created.")

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

    # Write to console
    print("[INFO] Writing to console...")
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    print("[INFO] Streaming query started. Waiting for termination...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
