from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CombineMinIO") \
    .getOrCreate()

# MinIO configuration
MINIO_ENDPOINT = "172.17.0.1:9000"
MINIO_ACCESS_KEY = "kStHEgiS0L8wSMHBoOq6"
MINIO_SECRET_KEY = "6uiWCp2tkHVA7dicuXawjI2fyhX5PtEKJwECSFaV"
MINIO_BUCKET = 'mybucket'


# Get previous day's date
previous_date_str = date_sub(current_date(), 1).cast("string")

# Load data from MinIO
df = spark.read.parquet(f"s3a://{MINIO_BUCKET}/stock_data/{previous_date_str}/")

# Save as CSV
df.write.mode("overwrite").csv(f"s3a://{MINIO_BUCKET}/stock_data_combined/{previous_date_str}.csv")

print(f"Data saved as CSV for date {previous_date_str}.")