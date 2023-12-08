# InvestmentAssistant

## Step 1: 
Start the docker containers: 
cd docker-images 
docker-compose up -d

## Step 2:
Start the KafkaProducer Watchdog
python3 KafkaProducer.py

## Step 3:
Start the KafkaConsumer Spark job
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --jars data/postgresql-42.7.1.jar  KafkaConsumerNormalizer.py

## Step 4:
Run the BinanceDataCollector script to load data into the datalake
python3 BinanceDataCollector.py

