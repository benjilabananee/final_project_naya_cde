from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import col, from_json

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('S3_cars_to_kafka') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()

# Define the schema for the event
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("name", StringType(), True),
    StructField("market", StringType(), True),
    StructField("locale", StringType(), True),
    StructField("primary_exchange", StringType(), True),
    StructField("type", StringType(), True),
    StructField("active", BooleanType(), True),
    StructField("currency_name", StringType(), True),
    StructField("cik", StringType(), True),
    StructField("last_updated_utc", StringType(), True)  # Use StringType for datetime strings
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "stock_meta_data_to_s3_test") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON data and apply schema
df = df.selectExpr("CAST(value AS STRING) as json") \
       .select(from_json(col("json"), schema).alias("data")) \
       .select("data.*")

# Write to Parquet
query = df.writeStream \
    .format("parquet") \
    .option("path", "s3a://spark/stock/metadata2") \
    .option("checkpointLocation", "s3a://spark/stock/metadata/checkpoint2") \
    .outputMode("append") \
    .start()

query.awaitTermination()