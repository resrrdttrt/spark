spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  --jars /home/vht/duonghdt/jars/postgresql-42.7.3.jar \
  minio_to_postgres_count_distance.py
