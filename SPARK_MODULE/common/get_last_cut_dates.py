from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col,max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import functions as F
import sys
sys.path.append('/home/developer/projects/spark-course-python/final_project_naya_cde')
import SPARK_MODULE.configuration as c
from datetime import datetime
import boto3 # type: ignore
import time
import json

def list_folders_in_partition(bucket_name: str, partition_prefix: str):

    s3_client = boto3.client('s3', endpoint_url=c.minio_server, aws_access_key_id=c.minio_access_key, aws_secret_access_key=c.minio_secret_key)
    
    paginator = s3_client.get_paginator('list_objects_v2')
    folder_paths = set()

    for page in paginator.paginate(Bucket=bucket_name, Prefix=partition_prefix, Delimiter='/'):
        for content in page.get('CommonPrefixes', []):
            folder_prefix = content.get('Prefix')
            if folder_prefix:
                folder_paths.add(folder_prefix)

    return folder_paths

def get_max_month_path(folders):
    max_month = -1
    max_month_path = None
    
    for path in folders:
        # Extract month from the path
        parts = path.split('/')
        month_part = next((p for p in parts if p.startswith('month=')), None)
        
        if month_part:
            month = int(month_part.split('=')[1])
            if month > max_month:
                max_month = month
                max_month_path = path
    
    return max_month_path

# Initialize Spark Session-
spark =  SparkSession \
        .builder \
        .master("local[*]") \
        .appName('kafka_stok') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
        .config("spark.hadoop.fs.s3a.endpoint", c.minio_server) \
        .config("spark.hadoop.fs.s3a.access.key", c.minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", c.minio_secret_key) \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

# Read Parquet file from S3
parquet_path_metadata = c.s3_metadata_cleaned
parquet_df_metadata = spark.read.parquet(parquet_path_metadata)

bucket_name = 'spark'
partition_prefix = f'stock/transaction/year={datetime.now().year}/'
parquet_df_transacions = spark.read.parquet(f"s3a://{bucket_name}/" + get_max_month_path(list_folders_in_partition(bucket_name, partition_prefix)))

# Assuming `result_df` is your DataFrame
max_transaction_date_df = parquet_df_transacions.agg(max("transaction_date").alias("max_transaction_date"))

# Collect the result
max_transaction_date = max_transaction_date_df.collect()[0]["max_transaction_date"]

print(max_transaction_date)

