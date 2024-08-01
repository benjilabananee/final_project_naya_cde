from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import col, from_json
import sys
sys.path.append('/home/developer/projects/spark-course-python/final_project_naya_cde')
import SPARK_MODULE.configuration as c

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession.
    """
    return SparkSession \
        .builder \
        .master("local[*]") \
        .appName(app_name) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
        .getOrCreate()

def define_schema() -> StructType:
    """
    Define and return the schema for the event data.
    """
    return StructType([
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

def read_from_kafka(spark: SparkSession, kafka_options: dict) :
    """
    Read streaming data from Kafka.
    """
    return spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

def process_data(df, schema: StructType) :
    """
    Process the incoming data from Kafka.
    """
    return df.selectExpr("CAST(value AS STRING) as json") \
             .select(from_json(col("json"), schema).alias("data")) \
             .select("data.*")

def write_to_parquet(df, parquet_options: dict):
    """
    Write the processed data to Parquet format.
    """
    query = df.writeStream \
        .format("parquet") \
        .options(**parquet_options) \
        .outputMode("append") \
        .start()
    
    query.awaitTermination()

if __name__ == '__main__':
    # Configuration
    app_name = 'S3_cars_to_kafka'
    kafka_options = {
        "kafka.bootstrap.servers": c.kafka_cluster,
        "subscribe": c.stock_metadata_topic,
        "startingOffsets": "earliest"
    }
    parquet_options = {
        "path": c.s3_metadata_path,
        "checkpointLocation": c.s3_metadata_checkpoint
    }

    # Execution
    spark = create_spark_session(app_name)
    schema = define_schema()
    df = read_from_kafka(spark, kafka_options)
    processed_df = process_data(df, schema)
    write_to_parquet(processed_df, parquet_options)