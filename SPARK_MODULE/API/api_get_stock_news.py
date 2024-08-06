import sys
sys.path.append('/home/developer/projects/spark-course-python/final_project_naya_cde')
import requests
import time
import json
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, from_json, arrays_zip,lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import SPARK_MODULE.configuration  as c
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Configuration
NEWS_FROM_DATE =datetime.now() - timedelta(days=100) 

BASE_URL = f"{c.stock_data_news}&apiKey={c.api_key}&published_utc.gt={NEWS_FROM_DATE.strftime('%Y-%m-%dT%H:%M:%SZ')}&ticker="
STOCKS_FOR_INVESTIGATION = c.stocks_for_investigation.split(',')
MAX_REQUESTS_PER_MINUTE = 5
result_data = []

params = {
    "adjusted": "true",
    "apiKey": c.api_key
}

def fetch_data(url: str, request_count:int) -> str:   
    while url:
        print(request_count)
        if request_count >= MAX_REQUESTS_PER_MINUTE:
            print("rated limit wait 60 second...")
            time.sleep(62)
            request_count = 0

        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            break

        data = response.json()
        if 'results' not in data:
            print(f"Error: 'results' key not found in response")
            break

        result_data.extend(data.get('results', []))

        next_url = data.get('next_url')
        if next_url and 'apiKey=' not in next_url:
            next_url = f"{next_url}&apiKey={c.api_key}"
        url = next_url
        request_count += 1

    return request_count 


def get_spark_session() -> SparkSession:
    return SparkSession.builder \
        .master("local[*]") \
        .appName('test_') \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()

def define_json_schema() -> ArrayType:
    return ArrayType(StructType([
        StructField('amp_url', StringType(), True),
        StructField('article_url', StringType(), True),
        StructField('author', StringType(), True),
        StructField('description', StringType(), True),
        StructField('id', StringType(), True),
        StructField('image_url', StringType(), True),
        StructField('insights', ArrayType(StructType([
            StructField('sentiment', StringType(), True),
            StructField('sentiment_reasoning', StringType(), True),
            StructField('ticker', StringType(), True)
        ]), True), True),
        StructField('keywords', ArrayType(StringType(), True), True),
        StructField('published_utc', StringType(), True),
        StructField('publisher', StructType([
            StructField('favicon_url', StringType(), True),
            StructField('homepage_url', StringType(), True),
            StructField('logo_url', StringType(), True),
            StructField('name', StringType(), True)
        ]), True),
        StructField('tickers', ArrayType(StringType(), True), True),
        StructField('title', StringType(), True)
    ]))

def transform_data(df: DataFrame) -> DataFrame:
    json_schema = define_json_schema()
    df_with_data = df.withColumn("data", from_json(col("json_data"), json_schema))
    df_exploded = df_with_data.select(explode(col("data")).alias("data_exploded"))
    df_result = df_exploded.select("data_exploded.*")

    df_ticker = df_result.select(
        col('title').alias('article_title'),
        col('publisher.name').alias('publisher'),
        col('author'),
        F.date_format(F.to_date(F.col('published_utc'), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''), 'yyyy-MM-dd').alias('publication_date'),
        col('insights.ticker').alias('ticker'),
        col('insights.sentiment')
    ).dropDuplicates()

    df_exploded_ticker = df_ticker.withColumn("data_exploded", arrays_zip("ticker", "sentiment"))
    df_ticker_2 = df_exploded_ticker.withColumn("data_exploded", explode("data_exploded"))

    return df_ticker_2.select(
        col("data_exploded.ticker").alias("ticker"),
        "article_title",
        "publisher",
        "author",
        "publication_date",
        col("data_exploded.sentiment").alias("sentiment"),
        col("ticker").alias("related_ticker")
    ).filter(col('ticker').isin(STOCKS_FOR_INVESTIGATION))

def write_to_postgres(df: DataFrame,connection_properties: dict, jdbc_url: str) -> None:
    df.write \
        .jdbc(url=jdbc_url, table="articles", mode="overwrite", properties=connection_properties)
    
if __name__ == '__main__':
    
    request_count = 0

    for stock in STOCKS_FOR_INVESTIGATION:
        #update the request_count in order to not fall because of limitation of the API and fill up the result_data
        request_count = fetch_data(BASE_URL + stock, request_count)
        
    json_data = json.dumps(result_data)

    spark = get_spark_session()
    df = spark.createDataFrame([(BASE_URL,)], ["url"])

    df_with_json_data = df.withColumn("json_data", lit(json_data))

    transformed_df = transform_data(df_with_json_data)

    jdbc_url = c.jdbc_url
    connection_properties = {
    "user": c.user_postgres,
    "password": c.password_postgres,
    "driver": c.driver_postgres
    }

    write_to_postgres(transformed_df, connection_properties, jdbc_url)