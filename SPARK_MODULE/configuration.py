######################################################################################
####################################GENERAL_KAFKA######################################
######################################################################################
kafka_cluster = "course-kafka:9092"
stock_data_topic="stock_data"
stock_news_topic="stock_news"
stock_metadata_topic="stock_meta_data_to_s3"

######################################################################################
####################################GENERAL_S3######################################
######################################################################################
s3_metadata_path = "s3a://spark/stock/metadata"
s3_metadata_checkpoint ="s3a://spark/stock/metadata/checkpoint"
s3_metadata_cleaned = "s3a://spark/stock/metadata_filtered"
s3_modified_transaction = "s3a://spark/stock/transaction"
s3_modified_transaction_checkpoint ="s3a://spark/stock/transaction/checkpoint"

######################################################################################
####################################URL_DATA##########################################
######################################################################################

stock_data_from_api = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
base_url_stock_meta_data = "https://api.polygon.io/v3/reference/tickers?active=true&limit=100&market=stocks&apiKey="
stock_data_news = "https://api.polygon.io/v2/reference/news?limit=1000" 
api_key = "4L8BLqY1mDFQIZnC0OzistwsUfrVHNKT"
stocks_for_investigation = "NVDA,ORCL,INTC,ARM"

######################################################################################
####################################MINIO_KEYS##########################################
######################################################################################

minio_access_key = "uIciYucooEkyAt3n"
minio_secret_key = "W1TZKOV2zT99lOOYYL7ld4kCfzj4RhOD"
minio_server = "http://minio:9000"

######################################################################################
####################################POSTGRES_DETAILS##################################
######################################################################################

jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
user_postgres = "postgres"
password_postgres = "postgres"
driver_postgres = "org.postgresql.Driver"
