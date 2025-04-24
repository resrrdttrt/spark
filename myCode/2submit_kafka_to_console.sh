spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0 \
    kafka_to_console.py
