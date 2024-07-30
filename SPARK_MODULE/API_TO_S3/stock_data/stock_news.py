from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import functions as F
import sys
sys.path.append('/home/developer/projects/spark-course-python/spark_course_python/final_project_naya_cde/')
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
    StructField("id", StringType(), True),
    StructField("publisher", StructType([
        StructField("name", StringType(), True),
        StructField("homepage_url", StringType(), True),
        StructField("logo_url", StringType(), True),
        StructField("favicon_url", StringType(), True)
    ]), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("published_utc", StringType(), True),  # Alternatively, use TimestampType() if converting to a timestamp
    StructField("article_url", StringType(), True),
    StructField("tickers", ArrayType(StringType()), True),
    StructField("image_url", StringType(), True),
    StructField("description", StringType(), True),
    StructField("keywords", ArrayType(StringType()), True),
    StructField("insights", ArrayType(StructType([
        StructField("ticker", StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("sentiment_reasoning", StringType(), True)
    ])), True)
])

# Set up Kafka source and read stream data
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", c.kafka_cluster) \
    .option("subscribe", c.stock_news_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON data from Kafka stream
kafka_parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("parsed_value")

)
df_ticker = kafka_parsed_df.select(col('parsed_value.title').alias('article_title'),
                                    col('parsed_value.publisher.name').alias('publisher'),
                                    col('parsed_value.author').alias('author'),
                                    F.date_format(F.to_date(F.col('parsed_value.published_utc'), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''), 'yyyy-MM-dd').alias('puclication_date'),
                                    F.explode(col('parsed_value.insights.ticker')).alias('ticker'),
                                    col('parsed_value.insights.sentiment')).filter(col('ticker') == 'ORCL')

df_sentiment = df_ticker.withColumn('sentiment',F.explode(col('sentiment'))).dropDuplicates()

query = df_sentiment.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .outputMode("append") \
    .start()

query.awaitTermination()


# Write the result to S3 in Parquet format with checkpointing
# query = result_df.writeStream \
#     .format("parquet") \
#     .option("path", c.s3_modified_transaction) \
#     .option("checkpointLocation", c.s3_modified_transaction_checkpoint) \
#     .partitionBy("year","month") \
#     .outputMode("append") \
#     .start()

# Await termination of the query
query.awaitTermination()