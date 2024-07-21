from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("S3ParquetProcessing") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .getOrCreate()

# Read data from Parquet files in S3
df = spark.read \
    .format("parquet") \
    .option("path", "s3a://spark/stock/transaction/transaction_date=2024-07-16") \
    .load()



print(df.count())

# Optionally, write the result to S3 or another location
df.write \
    .format("parquet") \
    .option("path", "s3a://spark/stock/metadata_filtered") \
    .mode("overwrite") \
    .save()



