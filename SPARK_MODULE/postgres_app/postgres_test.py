from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("PostgresConnectionExample") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# Database connection properties
jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Read data from PostgreSQL table
df = spark.read \
    .jdbc(url=jdbc_url, table="market_data", properties=connection_properties)

# Show the first few rows of the DataFrame
df.show()

# Perform some operations
# df_filtered = df.filter(df["market_data"] > 100)
df.show()

# Stop the SparkSession
spark.stop()