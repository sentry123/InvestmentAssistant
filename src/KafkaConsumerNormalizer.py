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


# Create a schema for the structured streaming DataFrame
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("symbol", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume_crypto", DoubleType()),
    StructField("volume_currency", DoubleType()),
    StructField("weighted_price", DoubleType())
])


# Define UDFs
format_timestamp_udf = udf(format_timestamp, StringType())
parse_json_string_udf = udf(parse_json_string, schema)


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


# Apply UDFs
result_df = df.withColumn("parsed_data", parse_json_string_udf(df["value"]))
result_df = result_df.withColumn("timestamp", format_timestamp_udf(result_df["parsed_data"]["timestamp"]))

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
