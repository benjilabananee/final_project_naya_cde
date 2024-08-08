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

def list_folders_in_partition(bucket_name: str, partition_prefix: str):

    s3_client = boto3.client('s3', endpoint_url=c.minio_server, aws_access_key_id=c.minio_access_key, aws_secret_access_key=c.minio_secret_key)
    
    paginator = s3_client.get_paginator('list_objects_v2')
    folder_paths = set()

    for page in paginator.paginate(Bucket=bucket_name, Prefix=partition_prefix, Delimiter='/'):
        for content in page.get('CommonPrefixes', []):
            folder_prefix = content.get('Prefix')
            if folder_prefix:
                folder_paths.add(folder_prefix)

    return folder_paths

def get_max_month_path(folders):
    max_month = -1
    max_month_path = None
    
    for path in folders:
        # Extract month from the path
        parts = path.split('/')
        month_part = next((p for p in parts if p.startswith('month=')), None)
        
        if month_part:
            month = int(month_part.split('=')[1])
            if month > max_month:
                max_month = month
                max_month_path = path
    
    return max_month_path

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

bucket_name = 'spark'
partition_prefix = f'stock/transaction/year={datetime.now().year}/'
parquet_df_transacions = spark.read.parquet(f"s3a://{bucket_name}/" + get_max_month_path(list_folders_in_partition(bucket_name, partition_prefix)))

# Assuming `result_df` is your DataFrame
max_transaction_date_df = parquet_df_transacions.agg(max("transaction_date").alias("max_transaction_date"))

# Collect the result
max_transaction_date = max_transaction_date_df.collect()[0]["max_transaction_date"]

print(f"Maximum transaction_date: {max_transaction_date}")

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

# # Write to console instead of S3
# query = result_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()

# Write the result to S3 in Parquet format with checkpointing
query = result_df.writeStream \
    .format("parquet") \
    .option("path", c.s3_modified_transaction) \
    .option("checkpointLocation", c.s3_modified_transaction_checkpoint) \
    .partitionBy("year","month") \
    .outputMode("append") \
    .start()

# Set the timeout duration (in seconds)
timeout_duration = 20  # For example, 1 hour

# Start the timer
start_time = time.time()

while query.isActive:
    elapsed_time = time.time() - start_time
    if elapsed_time >= timeout_duration:
        query.stop()
        print(f"Stream stopped after running for {timeout_duration} seconds.")
        break
    
    query.awaitTermination(10)  # Adjust timeout a