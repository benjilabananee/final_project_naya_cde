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
    .option("path", "s3a://spark/stock/metadata2") \
    .load()

# Remove duplicates
df_dedup = df.dropDuplicates()

count_after = df_dedup.count()
print(f"Count after deduplication: {count_after}")

#remove the colum of the date
filtered_df = df_dedup.drop("last_updated_utc")

# Optionally, write the result to S3 or another location
filtered_df.write \
    .format("parquet") \
    .option("path", "s3a://spark/stock/metadata_filtered") \
    .mode("overwrite") \
    .save()



