from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType,DataType

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('kafka_stock') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()

# Step 2: Read Parquet file from S3
parquet_path = "s3a://spark/stock/metadata_filtered"
parquet_df = spark.read.parquet(parquet_path)

# Step 3: Set up Kafka source to read JSON data
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "stock_data") \
    .option("startingOffsets", "earliest") \
    .load()

# Define the schema for the JSON data
schema = StructType([
    StructField("T", StringType(), True),        # 'T': string type
    StructField("v", IntegerType(), True),       # 'v': integer type
    StructField("vw", FloatType(), True),        # 'vw': float type
    StructField("o", FloatType(), True),         # 'o': float type
    StructField("c", FloatType(), True),         # 'c': float type
    StructField("h", FloatType(), True),         # 'h': float type
    StructField("l", FloatType(), True),         # 'l': float type
    StructField("n", IntegerType(), True),        # 'n': integer type
    StructField("date_time"), DataType
])

# Parse the JSON data
kafka_parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("parsed_value"))

# Flatten Kafka DataFrame and rename columns to avoid conflicts
kafka_extracted_df = kafka_parsed_df.select(
    col("parsed_value.T").alias("kafka_ticker"),
    col("parsed_value.v"),
    col("parsed_value.vw"),
    col("parsed_value.o"),
    col("parsed_value.c"),
    col("parsed_value.h"),
    col("parsed_value.l"),
    col("parsed_value.n"),
    col("date_time").alias("transaction_date")
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

# Perform the join operation
joined_df = kafka_extracted_df.join(
    parquet_renamed_df,
    kafka_extracted_df["kafka_ticker"] == parquet_renamed_df["parquet_ticker"],
    "inner"
)

# Select columns for the output
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
    kafka_extracted_df["v"].alias("volume"),
    kafka_extracted_df["vw"].alias("volume_weighted"),
    kafka_extracted_df["o"].alias("open_price"),
    kafka_extracted_df["c"].alias("close_price"),
    kafka_extracted_df["h"].alias("high_price"),
    kafka_extracted_df["l"].alias("low_price"),
    kafka_extracted_df["n"].alias("number_of_transaction")
)

# Output the results
query = result_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()