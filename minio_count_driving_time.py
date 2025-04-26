from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lag, collect_list, udf, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType, BooleanType, ArrayType, ByteType, DoubleType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from datetime import datetime

# Config variables
MINIO_ENDPOINT = "http://172.21.6.68:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "vtrackingsparkwithcountoffline"
INPUT_PATH = "parquet"  # Input path: s3a://vtrackingts/json
OUTPUT_PATH = "intraday"  # Output path: s3a://vtrackingts/intraday_reports
TIME_CONFIG = 900000  # Time threshold in ms (15 minutes)
TIME_GAP_THRESHOLD = 600000  # Time gap for segmentation in ms (10 minutes)

# Schema for VTrackingReportIntraday (output)
output_schema = StructType([
    StructField("Id", StringType(), True),
    StructField("Ts", LongType(), True),
    StructField("TsStartWeek", LongType(), True),
    StructField("TsEndWeek", LongType(), True),
    StructField("Geocoding", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("PrevTs", LongType(), True),
    StructField("PrevGeocoding", StringType(), True),
    StructField("PrevLatitude", FloatType(), True),
    StructField("PrevLongitude", FloatType(), True),
    StructField("StatusDuration", LongType(), True),
    StructField("PlateNo", StringType(), True),
    StructField("DriverName", StringType(), True),
    StructField("DriverLicense", StringType(), True),
    StructField("NumberOfTrips", LongType(), True),
    StructField("NotEndOfTrips", BooleanType(), True)
])

input_schema = StructType([
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
            StructField("day", StringType(), True)
])

def main():
    print("[INFO] Starting Spark batch job to process MinIO data and generate intraday reports")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MinIOToIntradayReportsBatch") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.apache.hadoop:hadoop-aws:3.3.6") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
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
    t1 = datetime.now()
    print("[INFO] Spark session initialized.")
    print(f"[INFO] Spark start reading {t1}.")
    # Read data from MinIO as a batch DataFrame
    input_df = spark.read \
        .format("parquet") \
        .schema(input_schema) \
        .load(f"s3a://{MINIO_BUCKET}/{INPUT_PATH}")
    
    # Filter null id record
    input_df = input_df.filter(col("id").isNotNull() & (col("id") != "__HIVE_DEFAULT_PARTITION__") & (col("id") == "8fb92b58-da6f-4da7-980f-549310e04a1b"))
    print("[INFO] Loaded batch data from MinIO.")

    # Step 1: Determine last non-historical driver_name and driver_license per id
    non_historical_df = input_df.filter(~col("history")).select("id", "ts", "driver_name", "driver_license")
    window_spec = Window.partitionBy("id").orderBy(col("ts").desc())
    last_driver_df = non_historical_df.withColumn("rn", F.row_number().over(window_spec)) \
                                     .filter(col("rn") == 1) \
                                     .select("id", col("driver_name").alias("last_driver_name"), 
                                             col("driver_license").alias("last_driver_license"))

    # Join last driver info and fill driver_name and driver_license
    input_df = input_df.join(last_driver_df, "id", "left") \
                       .withColumn("driver_name", F.coalesce(col("driver_name"), col("last_driver_name"))) \
                       .withColumn("driver_license", F.coalesce(col("driver_license"), col("last_driver_license"))) \
                       .drop("last_driver_name", "last_driver_license")

    # Step 2: Map status to numeric values (run/overspeed=1, stop/park=0, offline/badgps=2)
    input_df = input_df.withColumn("status_num", 
        when(col("status").isin("park", "stop"), 0)
        .when(col("status").isin("run", "overspeed"), 1)
        .otherwise(2).cast(ByteType()))

    # Step 3: Add edge case (duplicate last row with salt values)
    input_df = input_df.orderBy("id", "day", "ts")
    window_spec = Window.partitionBy("id", "day").orderBy(col("ts").desc())
    last_row_df = input_df.withColumn("rn", F.row_number().over(window_spec)) \
                          .filter(col("rn") == 1) \
                          .select(
                              "id", "day", "ts", "geocoding", "latitude", "longitude", 
                              "plate_no", "status_num", "status", "speed", "direction", 
                              "history", "driver_name", "driver_license"
                          ) \
                          .withColumn("driver_name", lit("saltD:VHT")) \
                          .withColumn("driver_license", lit("saltL:VHT")) \
                          .withColumn("plate_no", lit("saltP:VHT"))

    input_df = input_df.union(last_row_df)

    # Step 4: Segment data by driver changes or time gaps
    window_spec = Window.partitionBy("id", "day").orderBy("ts")
    input_df = input_df.withColumn("prev_driver_license", lag("driver_license").over(window_spec)) \
                       .withColumn("prev_ts", lag("ts").over(window_spec)) \
                       .withColumn("is_new_segment", 
                                   when(
                                       (col("driver_license") != col("prev_driver_license")) | 
                                       ((col("ts") - col("prev_ts") > TIME_GAP_THRESHOLD) & col("prev_ts").isNotNull()) | 
                                       col("prev_ts").isNull(), 
                                       1).otherwise(0))

    input_df = input_df.withColumn("segment_id", F.sum("is_new_segment").over(window_spec))

    # Step 5: Collect data into arrays for processing
    grouped_df = input_df.groupBy("id", "day", "segment_id").agg(
        collect_list(struct(
            col("ts"), col("geocoding"), col("latitude"), col("longitude"),
            col("plate_no"), col("status_num"), col("driver_name"), col("driver_license")
        )).alias("records")
    )

    # Step 6: UDF to process segments (replicate vtrackingProcessIntradayRP)
    def process_segment(records, time_config):
        if len(records) < 2:
            return []

        # Extract fields into arrays
        ts = [r["ts"] for r in records]
        geocoding = [r["geocoding"] for r in records]
        latitude = [r["latitude"] for r in records]
        longitude = [r["longitude"] for r in records]
        plate_no = [r["plate_no"] for r in records]
        status = [r["status_num"] for r in records]
        driver_name = [r["driver_name"] for r in records]
        driver_license = [r["driver_license"] for r in records]

        # Add edge case: duplicate last record with status=3
        len_input = len(ts)
        ts.append(ts[-1])
        latitude.append(latitude[-1])
        longitude.append(longitude[-1])
        plate_no.append(plate_no[-1])
        driver_name.append(driver_name[-1])
        driver_license.append(driver_license[-1])
        geocoding.append(geocoding[-1])
        status.append(3)

        # Step 1: Identify status changes
        flag_change_status = [0]
        status_map = []
        for i in range(1, len(status)):
            if status[i-1] != status[i]:
                status_map.append(status[i-1])
                flag_change_status.append(i)
        status_map.append(3)
        flag_change_status.append(flag_change_status[-1])

        # Step 2: Merge short stops (status=0) between running states (status=1)
        for i in range(1, len(status_map)-1):
            if (status_map[i] == 0 and status_map[i-1] == 1 and status_map[i+1] == 1 and 
                ts[flag_change_status[i+1]] - ts[flag_change_status[i]] <= time_config):
                status_map[i] = 1

        # Step 3: Generate trip reports
        results = []
        for i in range(len(status_map)):
            if status_map[i] not in [0, 2, 3]:
                for j in range(i + 1, len(status_map)):
                    if status_map[j] in [0, 2, 3]:
                        result = {
                            "Ts": ts[flag_change_status[j]],
                            "Geocoding": geocoding[flag_change_status[j]],
                            "Latitude": latitude[flag_change_status[j]],
                            "Longitude": longitude[flag_change_status[j]],
                            "PrevTs": ts[flag_change_status[i]],
                            "PrevGeocoding": geocoding[flag_change_status[i]],
                            "PrevLatitude": latitude[flag_change_status[i]],
                            "PrevLongitude": longitude[flag_change_status[i]],
                            "StatusDuration": ts[flag_change_status[j]] - ts[flag_change_status[i]],
                            "PlateNo": plate_no[flag_change_status[i]],
                            "DriverName": driver_name[flag_change_status[i]],
                            "DriverLicense": driver_license[flag_change_status[i]],
                            "NotEndOfTrips": status_map[j] == 3,
                            "TsStartWeek": 0,  # Placeholder
                            "TsEndWeek": 0,    # Placeholder
                            "NumberOfTrips": 0 # Placeholder
                        }
                        results.append(result)
                        i = j
                        break
        return results

    process_segment_udf = udf(lambda records: process_segment(records, TIME_CONFIG), 
                             ArrayType(output_schema))

    # Apply UDF and explode results
    result_df = grouped_df.withColumn("results", process_segment_udf(col("records"))) \
                          .select("id", "day", F.explode("results").alias("result")) \
                          .select(
                              col("id").alias("Id"),
                              col("day"),
                              col("result.Ts"),
                              col("result.TsStartWeek"),
                              col("result.TsEndWeek"),
                              col("result.Geocoding"),
                              col("result.Latitude"),
                              col("result.Longitude"),
                              col("result.PrevTs"),
                              col("result.PrevGeocoding"),
                              col("result.PrevLatitude"),
                              col("result.PrevLongitude"),
                              col("result.StatusDuration"),
                              col("result.PlateNo"),
                              col("result.DriverName"),
                              col("result.DriverLicense"),
                              col("result.NumberOfTrips"),
                              col("result.NotEndOfTrips")
                          )
    t2 = datetime.now()
    print(f"[INFO] Spark start wriring {t2}.")
    print(f"[INFO] Time  {t2-t1}.")
    # Step 7: Write results to MinIO as a batch
    print(f"[INFO] Writing results to MinIO bucket: s3a://{MINIO_BUCKET}/{OUTPUT_PATH}")
    result_df.write \
        .format("json") \
        .mode("overwrite") \
        .partitionBy("Id", "day") \
        .save(f"s3a://{MINIO_BUCKET}/{OUTPUT_PATH}")

    t3 = datetime.now()
    print(f"[INFO] Spark done {t3}.")
    print(f"[INFO] Time  {t3-t2}.")
    print("[INFO] Batch job completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()