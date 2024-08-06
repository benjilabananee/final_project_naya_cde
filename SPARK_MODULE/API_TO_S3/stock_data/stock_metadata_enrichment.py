from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import functions as F
import sys
sys.path.append('/home/developer/projects/spark-course-python/final_project_naya_cde')
import SPARK_MODULE.configuration as c

# Initialize Spark Session-
spark =  SparkSession \
        .builder \
        .master("local[*]") \
        .appName('kafka_stok') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
        .getOrCreate()

# Read Parquet file from S3
parquet_path = c.s3_metadata_cleaned
parquet_df = spark.read.parquet(parquet_path)

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
    .load()

# Parse JSON data from Kafka stream
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
)

# Rename columns in the Parquet DataFrame to avoid conflicts
parquet_renamed_df = parquet_df.select(
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

# Await termination of the query
query.awaitTermination()