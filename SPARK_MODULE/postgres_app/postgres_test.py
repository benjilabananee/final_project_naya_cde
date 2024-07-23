from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import max
from datetime import datetime, timedelta
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, DateType
)


def create_spark_session(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession.
    """
    return SparkSession \
        .builder \
        .master("local[*]") \
        .appName(app_name) \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
        .getOrCreate()


def read_parquet_from_s3(spark: SparkSession, path: str) -> DataFrame:
    """
    Read a parquet file from S3.
    """
    try:
        return spark.read.format("parquet").load(path)
    except Exception as e:
        print(f"Error reading path {path}: {e}")
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=None)


def get_monthly_paths(base_path: str, start_date: str, end_date: str) -> list:
    """
    Generate a list of S3 paths for each year and month between start_date and end_date.
    """
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    paths = []
    current_date = start_dt

    while current_date.date() <= end_dt.date():
        year = current_date.year
        month = current_date.month
        paths.append(f"{base_path}/year={year}/month={month}")
        
        if month == 12:
            current_date = current_date.replace(year=year + 1, month=1, day=1)
        else:
            current_date = current_date.replace(month=month + 1, day=1)

    return paths


def write_stock_transformed_data(df: DataFrame, connection_properties: dict):
    """
    Write the transformed stock data to a PostgreSQL database.
    """
    if df.isEmpty():
        print("No data to write to PostgreSQL.")
        return

    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"

    df.write \
        .jdbc(url=jdbc_url, table="market_data", mode="append", properties=connection_properties)


def write_the_last_cut_date(df: DataFrame, spark: SparkSession, connection_properties: dict):
    """
    Write the last cut date to a PostgreSQL database.
    """
    if df.isEmpty():
        print("No data to write the last cut date.")
        return

    max_date = df.agg(max("transaction_date")).collect()[0][0]
    date_df = spark.createDataFrame([Row(CUT_DATE=max_date)])

    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"

    date_df.write \
        .jdbc(url=jdbc_url, table="cutting_dates", mode="append", properties=connection_properties)


def get_max_date_from_db(spark: SparkSession, connection_properties: dict) -> str:
    """
    Get the maximum date from a specified table in the PostgreSQL database.
    """
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    
    max_date_df = spark.read \
        .jdbc(url=jdbc_url, table="cutting_dates", properties=connection_properties) \
        .agg(max("CUT_DATE").alias("max_date"))

    max_date = max_date_df.collect()[0]["max_date"]
    return max_date.strftime('%Y-%m-%d') if max_date else None


def main():
    app_name = "S3ParquetProcessing"
    parquet_base_path = "s3a://spark/stock/transaction"

    connection_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    try:
        spark = create_spark_session(app_name)
        last_cut_date = get_max_date_from_db(spark, connection_properties)
        current_date = datetime.now().strftime('%Y-%m-%d')

        schema = StructType([
            StructField("name", StringType(), True),
            StructField("market", StringType(), True),
            StructField("locale", StringType(), True),
            StructField("primary_exchange", StringType(), True),
            StructField("type", StringType(), True),
            StructField("active", BooleanType(), True),
            StructField("currency_name", StringType(), True),
            StructField("cik", StringType(), True),
            StructField("ticker", StringType(), True),
            StructField("volume", DoubleType(), True),
            StructField("volume_weighted", DoubleType(), True),
            StructField("open_price", DoubleType(), True),
            StructField("close_price", DoubleType(), True),
            StructField("high_price", DoubleType(), True),
            StructField("low_price", DoubleType(), True),
            StructField("number_of_transaction", IntegerType(), True),
            StructField("transaction_date", DateType(), True)
        ])

        if last_cut_date:
            s3_paths = get_monthly_paths(parquet_base_path, last_cut_date, current_date)

            if not s3_paths:
                print("No S3 paths generated.")
                return

            combined_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=schema)

            for path in s3_paths:
                df = read_parquet_from_s3(spark, path)
                if not df.isEmpty():
                    combined_df = combined_df.union(df)

            if combined_df.isEmpty():
                print("No data combined from S3 paths.")
            else:
                write_stock_transformed_data(combined_df, connection_properties)
                write_the_last_cut_date(combined_df, spark, connection_properties)
        else:
            print("No last cut date found in the database.")
    
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
