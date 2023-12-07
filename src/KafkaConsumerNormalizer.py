from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from datetime import datetime, timezone, timedelta


##JDBC driver for db connectivity
jdbc_url = "jdbc:postgresql://your-timescaledb-host:your-port/your-database"
connection_properties = {
    "user": "your-username",
    "password": "your-password",
    "driver": "org.postgresql.Driver"
}


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
        formatted_data = {'timestamp': format_timestamp(data['openTime']), 'symbol': data['symbol'],
                          'open': data['openPrice'], 'high': data['highPrice'], 'low': data['lowPrice'],
                          'close': data['prevClosePrice'], 'volume_crypto': data['volume'],
                          'volume_currency': data['quoteVolume'], 'weighted_price': data['weightedAvgPrice']}

        return formatted_data

    except ValueError:
        return None


def filter_symbol_udf(symbol):
    return symbol.endswith("USDT")


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
filter_symbol_udf = udf(filter_symbol_udf, BooleanType())


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
filtered_df = result_df.filter(filter_symbol_udf(result_df["parsed_data"]["symbol"]))


#Save dataframe to db
filtered_df.write \
    .jdbc(url=jdbc_url, table="your-table-name", mode="append", properties=connection_properties)


# Await termination of the query
query.awaitTermination()
spark.stop()
