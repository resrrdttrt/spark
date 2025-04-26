nohup spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --master spark://172.21.6.63:7077 \
  --executor-cores 1 \
  --executor-memory 1g \
  --total-executor-cores 1 \
  --conf spark.driver.host=172.21.6.63 \
  kafka_to_minio_with_count_offline.py >> /home/vht/duonghdt/spark_log/log_kafka_to_minio_with_count_offline.log 2>&1 &