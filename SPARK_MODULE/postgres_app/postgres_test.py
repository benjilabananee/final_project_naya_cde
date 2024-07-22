from pyspark.sql import SparkSession

def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession \
        .builder \
        .master("local[*]") \
        .appName(app_name) \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
        .getOrCreate()

def read_parquet_from_s3(spark: SparkSession, path: str):

    return spark.read \
        .format("parquet") \
        .load(path)

def write_stock_transformed_data(df):

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
