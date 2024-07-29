######################################################################################
####################################GENERAL_KAFKA######################################
######################################################################################
kafka_cluster = "course-kafka:9092"
stock_data_topic="stock_data"
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
api_key = "4L8BLqY1mDFQIZnC0OzistwsUfrVHNKT"

######################################################################################
####################################MINIO_KEYS##########################################
######################################################################################

minio_access_key = "RGw8lfP8gExTCS7C"
minio_secret_key = "BhTorYGmvKmm4hpvPrESoLbMP3DMMa1g"
minio_server = "http://minio:9000"