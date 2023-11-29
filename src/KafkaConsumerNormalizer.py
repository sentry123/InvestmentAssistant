from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split


spark = SparkSession.builder \
    .appName("KafkaSparkStreaming")     \
    .config("spark.jars", "data/spark-sql-kafka-0-10_2.12-3.4.1.jar") \
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
