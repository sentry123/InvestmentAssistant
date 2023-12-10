import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType


logging.basicConfig(filename='./logs/pyspark.log', level=logging.INFO)

def write_to_timescaledb(batch_df, batch_id):
    batch_df.write \
        .mode("append") \
        .jdbc(url=jdbc_url, table="crypto_price_data", properties=connection_properties)


# JDBC driver for db connectivity
jdbc_url = "jdbc:postgresql://localhost:5432/mydatabase"
connection_properties = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "org.postgresql.Driver"
}


log4j_properties_path = os.path.abspath('log4j.properties')

# Create a Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.driver.extraJavaOptions", f"-Dlog4j.configuration=file:{log4j_properties_path}") \
    .getOrCreate()

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)
logger.info("Spark Logger Initialized")



# Define the Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "test"

# Define the schema for the structured streaming DataFrame
schema = StructType([
    StructField("symbol", StringType()),
    StructField("priceChange", StringType()),
    StructField("priceChangePercent", StringType()),
    StructField("weightedAvgPrice", StringType()),
    StructField("prevClosePrice", StringType()),
    StructField("lastPrice", StringType()),
    StructField("lastQty", StringType()),
    StructField("bidPrice", StringType()),
    StructField("bidQty", StringType()),
    StructField("askPrice", StringType()),
    StructField("askQty", StringType()),
    StructField("openPrice", StringType()),
    StructField("highPrice", StringType()),
    StructField("lowPrice", StringType()),
    StructField("volume", StringType()),
    StructField("quoteVolume", StringType()),
    StructField("openTime", StringType()),
    StructField("closeTime", StringType()),
    StructField("firstId", StringType()),
    StructField("lastId", StringType()),
    StructField("count", StringType())
])

# Define the input DataFrame representing the stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Decode binary data into a string
df = df.withColumn("decoded_value", col("value").cast("string"))

# Parse JSON string into struct
df = df.withColumn("parsed_data", from_json(df["decoded_value"], schema))

# Select individual fields
result_df = df.select(
    "parsed_data.symbol",
    "parsed_data.weightedAvgPrice",
    "parsed_data.prevClosePrice",
    "parsed_data.openPrice",
    "parsed_data.highPrice",
    "parsed_data.lowPrice",
    "parsed_data.volume",
    "parsed_data.quoteVolume",
    "parsed_data.openTime"
)

# Filter based on symbol
filtered_df = result_df.filter(result_df["symbol"].endswith("USDT"))

# Format columns and cast data types
formatted_result_df = filtered_df.select(
    col("openTime").cast("timestamp").alias("timestamp"),
    col("symbol"),
    col("openPrice").cast("double").alias("open"),
    col("highPrice").cast("double").alias("high"),
    col("lowPrice").cast("double").alias("low"),
    col("prevClosePrice").cast("double").alias("close"),
    col("volume").cast("double").alias("volume_crypto"),
    col("quoteVolume").cast("double").alias("volume_currency"),
    col("weightedAvgPrice").cast("double").alias("weighted_price")
)

# Define a query to write to TimescaleDB and print the records to the console
query = formatted_result_df.writeStream.outputMode("append").foreachBatch(write_to_timescaledb).start()

# Await termination of the query
query.awaitTermination()
