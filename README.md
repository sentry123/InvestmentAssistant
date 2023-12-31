# InvestmentAssistant


## Steps to get the data
Create API keys in Binance portal <br>
Use the below link to make windowed queries to get the data <br>
<li>https://binance-docs.github.io/apidocs/spot/en/#rolling-window-price-change-statistics</li>


## Follow the below steps to run the application
### Step 1: 
Start the docker containers:  <br>
`cd docker-images` <br>
`docker-compose up -d` <br>

### Step 2:
Start the KafkaProducer Watchdog <br>
`python3 KafkaProducer.py` <br>

### Step 3:
Start the KafkaConsumer Spark job <br>
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --jars data/postgresql-42.7.1.jar  KafkaConsumerNormalizer.py` <br>

### Step 4:
Run the BinanceDataCollector script to load data into the datalake <br>
`python3 BinanceDataCollector.py` <br>

### Step 5:
Run the ipynb notebook Bitcoin_price_prediction_model.ipynb to view the outputs.
