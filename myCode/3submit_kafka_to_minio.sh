spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  kafka_to_minio.py
