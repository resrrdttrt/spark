from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SimpleApp") \
    .getOrCreate()

# Embedded data
data = [
    ("apple",),
    ("banana",),
    ("apple",),
    ("orange",),
    ("banana",),
    ("banana",)
]

# Create DataFrame
df = spark.createDataFrame(data, ["fruit"])

# Count occurrences of each fruit
fruit_count = df.groupBy("fruit").count()

# Show the result
fruit_count.show()

# Stop the Spark session
spark.stop()
