from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from datetime import datetime, timezone, timedelta


## UDFs - For data formatting

def format_timestamp(timestamp_ms):
    timestamp_seconds = timestamp_ms / 1000.0
    dt_utc = datetime.utcfromtimestamp(timestamp_seconds).replace(tzinfo=timezone.utc)
    postgres_timestamp = dt_utc.strftime('%Y-%m-%d %H:%M:%S %z')
    return postgres_timestamp


def parse_json_string(json_string):
    import json
    try:
        data = json.loads(json_string)
        formatted_data = {}
        formatted_data['timestamp'] = format_timestamp(data['openTime'])
        formatted_data['symbol'] = data['symbol']  
        formatted_data['open'] = data['openPrice']
        formatted_data['high'] = data['highPrice']
        formatted_data['low'] = data['lowPrice']
        formatted_data['close'] = data['prevClosePrice']
        formatted_data['volume_crypto'] = data['volume']
        formatted_data['volume_currency'] = data['quoteVolume']
        formatted_data['weighted_price'] = data['weightedAvgPrice']

        return formatted_data

    except ValueError:
        return None


# Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming")     \
    .config("spark.jars", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()


# Define the Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "test"


# Define the input DataFrame representing the stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()


# Convert the binary value from Kafka into a string
value_df = df.selectExpr("CAST(value AS STRING)")


# Split the string into words
words = value_df.select(
    explode(split(value_df.value, " ")).alias("word")
)


# Count the occurrences of each word
word_counts = words.groupBy("word").count()


# Display the word counts to the console
query = word_counts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()


# Await termination of the query
query.awaitTermination()
spark.stop()
