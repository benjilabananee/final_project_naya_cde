from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession.
    """
    return SparkSession \
        .builder \
        .master("local[*]") \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()

def read_parquet_from_s3(spark: SparkSession, path: str):
    """
    Read data from Parquet files in S3.
    """
    return spark.read \
        .format("parquet") \
        .load(path)

def remove_duplicates(df):
    """
    Remove duplicates from the DataFrame.
    """
    return df.dropDuplicates()

def count_records(df):
    """
    Count the number of records in the DataFrame.
    """
    return df.count()

def drop_column(df, column_name: str):
    """
    Drop a column from the DataFrame.
    """
    return df.drop(column_name)

def write_parquet_to_s3(df, path: str):
    """
    Write the DataFrame to Parquet files in S3.
    """
    df.write \
        .format("parquet") \
        .option("path", path) \
        .mode("overwrite") \
        .save()

if __name__ == '__main__':
    # Configuration
    app_name = "S3ParquetProcessing"
    parquet_path = "s3a://spark/stock/metadata"
    filtered_parquet_path = "s3a://spark/stock/metadata_filtered"
    column_to_drop = "last_updated_utc"

    # Execution
    try:
        spark = create_spark_session(app_name)
        df = read_parquet_from_s3(spark, parquet_path)
        
        # Remove duplicates
        df_dedup = remove_duplicates(df)
        
        # Count records after deduplication
        count_after = count_records(df_dedup)
        print(f"Count after deduplication: {count_after}")
        
        # Remove the column
        filtered_df = drop_column(df_dedup, column_to_drop)
        
        # Write the result to S3
        write_parquet_to_s3(filtered_df, filtered_parquet_path)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        spark.stop()  # Stop the Spark session