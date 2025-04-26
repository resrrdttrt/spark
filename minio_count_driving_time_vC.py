from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, FloatType, IntegerType, ArrayType, BooleanType
from pyspark.sql.functions import col, round, when, lag, lit, udf, struct, collect_list, explode, array, expr, concat_ws, first, to_date, from_unixtime
# Config variables
MINIO_ENDPOINT = "http://172.21.6.68:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_INPUT_BUCKET = "vtrackingsparkwithcountoffline"
MINIO_INPUT_PATH = "parquet"
MINIO_OUTPUT_BUCKET = "vtrackingsparkwithcountoffline"
MINIO_OUTPUT_PATH = "intraday"
TIME_CONFIG = 15 * 60  # 15 minutes in seconds

# Define schema for input data
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

# Schema for output data
output_schema = StructType([
    StructField("id", StringType(), True),
    StructField("ts", LongType(), True),
    StructField("geocoding", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("prev_ts", LongType(), True),
    StructField("prev_geocoding", StringType(), True),
    StructField("prev_latitude", FloatType(), True),
    StructField("prev_longitude", FloatType(), True),
    StructField("status_duration", LongType(), True),
    StructField("plate_no", StringType(), True),
    StructField("driver_name", StringType(), True),
    StructField("driver_license", StringType(), True),
    StructField("not_end_of_trips", BooleanType(), True)
])

def map_status(status):
    """Map status string to integer codes as in Go code"""
    if status in ["park", "stop"]:
        return 0
    elif status in ["run", "overspeed"]:
        return 1
    else:
        return 2

# UDF to process a group of records for a section
def process_intraday_section(records_list):
    """
    Process a section of records with the same driver license
    Implements the logic in vtrackingProcessIntradayRP from Go code
    """
    # Sort records by timestamp
    records = sorted(records_list, key=lambda r: r["ts"])
    
    if len(records) <= 1:
        return []
    
    # Prepare arrays similar to Go code
    ts = [r["ts"] for r in records]
    latitude = [r["latitude"] for r in records]
    longitude = [r["longitude"] for r in records]
    plate_no = [r["plate_no"] for r in records]
    status = [r["status_code"] for r in records]
    driver_name = [r["driver_name"] for r in records]
    driver_license = [r["driver_license"] for r in records]
    geocoding = [r["geocoding"] for r in records]
    
    # Add edge case as in Go code
    last_idx = len(ts) - 1
    ts.append(ts[last_idx])
    latitude.append(latitude[last_idx])
    longitude.append(longitude[last_idx])
    plate_no.append(plate_no[last_idx])
    driver_name.append(driver_name[last_idx])
    driver_license.append(driver_license[last_idx])
    geocoding.append(geocoding[last_idx])
    status.append(3)  # Special end marker
    
    # Find status change points
    flag_change_status = [0]
    status_map = []
    
    for i in range(1, len(status)):
        if status[i-1] != status[i]:
            status_map.append(status[i-1])
            flag_change_status.append(i)
    
    # Add edge case
    status_map.append(3)
    flag_change_status.append(flag_change_status[-1])
    
    # Merge parts that have time <= TIME_CONFIG
    for i in range(1, len(status_map) - 1):
        if (status_map[i] == 0 and 
            status_map[i-1] == 1 and 
            status_map[i+1] == 1 and 
            ts[flag_change_status[i+1]] - ts[flag_change_status[i]] <= TIME_CONFIG * 1000):
            status_map[i] = 1
    
    # Process and create results
    results = []
    i = 0
    while i < len(status_map):
        if status_map[i] != 1:  # Only process movement segments
            i += 1
            continue
            
        for j in range(i + 1, len(status_map)):
            if status_map[j] in [0, 2, 3]:  # Look for stop or end
                idx_i = flag_change_status[i]
                idx_j = flag_change_status[j]
                
                # Create a result record
                result = {
                    "id": records[0]["entity_id"],
                    "ts": ts[idx_j],
                    "geocoding": geocoding[idx_j],
                    "latitude": float(latitude[idx_j]),
                    "longitude": float(longitude[idx_j]),
                    "prev_ts": ts[idx_i],
                    "prev_geocoding": geocoding[idx_i],
                    "prev_latitude": float(latitude[idx_i]),
                    "prev_longitude": float(longitude[idx_i]),
                    "status_duration": ts[idx_j] - ts[idx_i],
                    "plate_no": plate_no[idx_i],
                    "driver_name": driver_name[idx_i],
                    "driver_license": driver_license[idx_i],
                    "not_end_of_trips": (status_map[j] == 3)
                }
                results.append(result)
                i = j
                break
        i += 1
        
    return results

def main():
    print("[INFO] Starting Spark job to process vehicle tracking data")
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("VehicleTrackingReportIntraday") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.6") \
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
    
    # Read data from MinIO
    print(f"[INFO] Reading data from MinIO: s3a://{MINIO_INPUT_BUCKET}/{MINIO_INPUT_PATH}")
    df = spark.read.schema(input_schema).json(f"s3a://{MINIO_INPUT_BUCKET}/{MINIO_INPUT_PATH}")
    
    # Define time range parameters (assuming we want to process data for a time range)
    # This can be passed as parameters or calculated based on current time
    start_time = 0  # Replace with actual start time if needed
    end_time = 9999999999999  # Replace with actual end time if needed
    
    # Filter data by time range if needed
    df = df.filter((col("ts") >= start_time) & (col("ts") <= end_time))
    
    # Prepare data similar to the Go code
    process_df = df.withColumn("status_code", 
        when(col("status").isin("park", "stop"), 0)
        .when(col("status").isin("run", "overspeed"), 1)
        .otherwise(2)
    )
    
    # Round latitude and longitude to 6 decimal places as in Go code
    process_df = process_df.withColumn("latitude", round(col("latitude"), 6)) \
                          .withColumn("longitude", round(col("longitude"), 6))
    
    # Define window for tracking driver changes
    window_spec = Window.partitionBy("id").orderBy("ts")
    
    # Identify driver changes and large time gaps
    process_df = process_df.withColumn("prev_driver_license", lag("driver_license").over(window_spec)) \
                          .withColumn("prev_ts", lag("ts").over(window_spec)) \
                          .withColumn("section_start", 
                                     (col("driver_license") != col("prev_driver_license")) | 
                                     (col("ts") - col("prev_ts") > 600000) | 
                                     col("prev_driver_license").isNull())
    
    # Generate section IDs
    process_df = process_df.withColumn("section_id", 
                                      expr("sum(cast(section_start as int)) over (partition by id order by ts)"))
    
    # Create a key for each section similar to the Go code
    process_df = process_df.withColumn("section_key", 
                                     concat_ws(":", col("plate_no"), col("ts").cast("string"), col("driver_name")))
    
    # Register UDF
    process_section_udf = udf(process_intraday_section, ArrayType(output_schema))
    
    # Group by entity_id and section_id to create sections
    section_df = process_df.groupBy("id", "section_id").agg(
        collect_list(
            struct(
                col("id").alias("entity_id"),
                col("ts"),
                col("latitude"),
                col("longitude"),
                col("plate_no"),
                col("status_code"),
                col("driver_name"),
                col("driver_license"),
                col("geocoding"),
                col("history")
            )
        ).alias("section_data"),
        first("section_key").alias("key")
    )
    
    # Process each section using the UDF
    result_df = section_df.withColumn("processed_results", process_section_udf(col("section_data"))) \
                         .withColumn("result", explode(col("processed_results"))) \
                         .select(
                             col("result.id"),
                             col("result.ts"),
                             col("result.geocoding"),
                             col("result.latitude"),
                             col("result.longitude"),
                             col("result.prev_ts"),
                             col("result.prev_geocoding"),
                             col("result.prev_latitude"),
                             col("result.prev_longitude"),
                             col("result.status_duration"),
                             col("result.plate_no"),
                             col("result.driver_name"),
                             col("result.driver_license"),
                             col("result.not_end_of_trips"),
                             to_date(from_unixtime(col("result.ts") / 1000)).alias("day")
                         )
    
    # Write results to MinIO
    print(f"[INFO] Writing results to MinIO: s3a://{MINIO_OUTPUT_BUCKET}/{MINIO_OUTPUT_PATH}")
    result_df.write \
        .mode("append") \
        .partitionBy("id", "day") \
        .json(f"s3a://{MINIO_OUTPUT_BUCKET}/{MINIO_OUTPUT_PATH}")
    
    print("[INFO] Processing completed successfully")
    spark.stop()

if __name__ == "__main__":
    main()