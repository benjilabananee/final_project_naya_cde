from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col,max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import functions as F
import sys
sys.path.append('/home/developer/projects/spark-course-python/final_project_naya_cde')
import SPARK_MODULE.configuration as c
from datetime import datetime
import boto3 # type: ignore
import time
import base64


# Initialize Spark Session-
spark =  SparkSession \
        .builder \
        .master("local[*]") \
        .appName('kafka_stok') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
        .config("spark.hadoop.fs.s3a.endpoint", c.minio_server) \
        .config("spark.hadoop.fs.s3a.access.key", c.minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", c.minio_secret_key) \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

# Read Parquet file from S3
parquet_path_metadata = c.s3_metadata_cleaned
parquet_df_metadata = spark.read.parquet(parquet_path_metadata)

parquet_df_metadata.cache()

decoded_bytes_last_cut_date = base64.b64decode(sys.argv[1])
decoded_string_last_cut_date = decoded_bytes_last_cut_date.decode('utf-8')

max_transaction_date = datetime.strptime(decoded_string_last_cut_date.strip(), "%Y-%m-%d") 

# Define schema for JSON data from Kafka
schema = StructType([
    StructField("T", StringType(), True),
    StructField("v", IntegerType(), True),
    StructField("vw", FloatType(), True),
    StructField("o", FloatType(), True),
    StructField("c", FloatType(), True),
    StructField("h", FloatType(), True),
    StructField("l", FloatType(), True),
    StructField("n", IntegerType(), True),
    StructField("date_time", DateType(), True)
])

# Set up Kafka source and read stream data
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", c.kafka_cluster) \
    .option("subscribe", c.stock_data_topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false")\
    .load()

# # Parse JSON data from Kafka stream
kafka_parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("parsed_value")
)

# Extract and rename columns from Kafka DataFrame
kafka_extracted_df = kafka_parsed_df.select(
    col("parsed_value.T").alias("kafka_ticker"),
    col("parsed_value.v").alias("volume"),
    col("parsed_value.vw").alias("volume_weighted"),
    col("parsed_value.o").alias("open_price"),
    col("parsed_value.c").alias("close_price"),
    col("parsed_value.h").alias("high_price"),
    col("parsed_value.l").alias("low_price"),
    col("parsed_value.n").alias("number_of_transaction"),
    col("parsed_value.date_time").alias("transaction_date")
).filter(col("transaction_date") > max_transaction_date)

# # Rename columns in the Parquet DataFrame to avoid conflicts
parquet_renamed_df = parquet_df_metadata.select(
    col("ticker").alias("parquet_ticker"),
    col("name"),
    col("market"),
    col("locale"),
    col("primary_exchange"),
    col("type"),
    col("active"),
    col("currency_name"),
    col("cik")
)

# Join Kafka and Parquet DataFrames on ticker
joined_df = kafka_extracted_df.join(
    parquet_renamed_df,
    kafka_extracted_df["kafka_ticker"] == parquet_renamed_df["parquet_ticker"],
    "inner"
)

# Select final columns for the output
result_df = joined_df.select(
    parquet_renamed_df["name"],
    parquet_renamed_df["market"],
    parquet_renamed_df["locale"],
    parquet_renamed_df["primary_exchange"],
    parquet_renamed_df["type"],
    parquet_renamed_df["active"],
    parquet_renamed_df["currency_name"],
    parquet_renamed_df["cik"],
    kafka_extracted_df["kafka_ticker"].alias("ticker"),
    kafka_extracted_df["volume"],
    kafka_extracted_df["volume_weighted"],
    kafka_extracted_df["open_price"],
    kafka_extracted_df["close_price"],
    kafka_extracted_df["high_price"],
    kafka_extracted_df["low_price"],
    kafka_extracted_df["number_of_transaction"],
    kafka_extracted_df["transaction_date"]
)

result_df = result_df.withColumn("year", F.year(F.col("transaction_date")))
result_df = result_df.withColumn("month", F.month(F.col("transaction_date")))

# Write the result to S3 in Parquet format with checkpointing
query = result_df.writeStream \
    .format("parquet") \
    .option("path", c.s3_modified_transaction) \
    .option("checkpointLocation", c.s3_modified_transaction_checkpoint) \
    .partitionBy("year","month") \
    .outputMode("append") \
    .start()

# query = result_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()

# Set the timeout duration (in seconds)
timeout_duration = 350  # For example, 1 hour
# Start the timer
start_time = time.time()

while query.isActive:
    elapsed_time = time.time() - start_time
    if elapsed_time >= timeout_duration:
        query.stop()
        print(f"Stream stopped after running for {timeout_duration} seconds.")
        break
    
    query.awaitTermination(10)  # Adjust timeout a
