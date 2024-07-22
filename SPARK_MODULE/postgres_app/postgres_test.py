from pyspark.sql import SparkSession

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession.
    
    Parameters:
        app_name (str): The name of the Spark application.
        
    Returns:
        SparkSession: A configured SparkSession.
    """
    return SparkSession \
        .builder \
        .master("local[*]") \
        .appName(app_name) \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
        .getOrCreate()

def read_parquet_from_s3(spark: SparkSession, path: str):
    """
    Read data from Parquet files in S3.
    
    Parameters:
        spark (SparkSession): The active SparkSession.
        path (str): The S3 path to the Parquet files.
        
    Returns:
        DataFrame: A Spark DataFrame containing the data.
    """
    return spark.read \
        .format("parquet") \
        .load(path)

def write_stock_transformed_data(df):
    """
    Write the transformed stock data to a PostgreSQL database.
    
    Parameters:
        df (DataFrame): The DataFrame to be written to the database.
    """
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    connection_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    
    df.write \
        .jdbc(url=jdbc_url, table="market_data", mode="append", properties=connection_properties)

if __name__ == '__main__':
    app_name = "S3ParquetProcessing"
    parquet_path = "s3a://spark/stock/transaction"

    try:
        spark = create_spark_session(app_name)
        df = read_parquet_from_s3(spark, parquet_path)
        # df.show()
        write_stock_transformed_data(df)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        spark.stop()  # Stop the Spark session
