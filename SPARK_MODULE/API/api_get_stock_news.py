import requests
import sys
import time
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode,from_json,arrays_zip
from pyspark.sql.types import  StructType, StructField, StringType, ArrayType, IntegerType, BooleanType, DateType
# Ensure you adjust the path to your configuration
sys.path.append('/home/developer/projects/spark-course-python/spark_course_python/final_project_naya_cde/')
sys.path.append('/home/developer/')
import SPARK_MODULE.configuration as c
from pyspark.sql import functions as F

# Configuration
BASE_URL = f"{c.stock_data_news}{c.api_key}"
MAX_REQUESTS_PER_MINUTE = 5

params = {
    "adjusted": "true",
    "apiKey": c.api_key  # Assuming you have moved the API key to the configuration file
}


def fetch_data(url):
    request_count = 0
    result_data = []

    while url:
        print(url)

        if request_count >= MAX_REQUESTS_PER_MINUTE:
            time.sleep(62)  # wait for 60 seconds
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

    return json.dumps(result_data)

if __name__ == '__main__':
    # # Create the UDF
    fetch_data_udf = udf(fetch_data, StringType())

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
        .getOrCreate()
    
    df = spark.createDataFrame([(BASE_URL,)], ["url"])
    

    # Apply the UDF to get JSON data
    df_with_json_data = df.withColumn("json_data", fetch_data_udf(df['url']))
    df_with_json_data.cache()

    json_schema = ArrayType(StructType([
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

    df_with_data = df_with_json_data.withColumn("data", from_json(col("json_data"), json_schema))

    df_exploded = df_with_data.select(explode(col("data")).alias("data_exploded"))

    df_result = df_exploded.select("data_exploded.*")

    df_ticker = df_result.select(col('title').alias('article_title'),
                           col('publisher.name').alias('publisher'),
                           col('author'),
                           F.date_format(F.to_date(F.col('published_utc'), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''), 'yyyy-MM-dd').alias('publication_date'),
                           col('insights.ticker').alias('ticker'),
                           col('insights.sentiment'))

    df_exploded_ticker = df_ticker.withColumn("data_exploded", arrays_zip("ticker", "sentiment")) 
    df_ticker_2 = df_exploded_ticker.withColumn("data_exploded", explode("data_exploded"))

    df_ticker_2 = df_ticker_2.select(
                                "article_title",
                                "publisher",
                                "author",
                                "publication_date",
                                col("data_exploded.ticker").alias("ticker"),
                                col("data_exploded.sentiment").alias("sentiment")).filter(col('ticker') == 'ORCL')

    df_ticker_2.show() 