import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kafka import KafkaProducer
from datetime import datetime, timezone
import os
import json
import re


# Kafka producer configuration
kafka_bootstrap_servers = 'kafka_bootstrap_servers'
kafka_topic = 'kafka_topic'
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

# Directory to monitor
directory_to_monitor = "{}/data".format(os.getcwd())

# Keep track of the latest processed timestamp
latest_timestamp = None


class FileEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        file_timestamp = extract_timestamp_from_file(file_path)

        global latest_timestamp
        if latest_timestamp is None or file_timestamp > latest_timestamp:
            # New file found
            latest_timestamp = file_timestamp
            send_file_to_kafka(file_path)


def extract_timestamp_from_file(file_path):
    pattern = r'(\d{13})'
    match = re.findall(pattern, file_path)[0]
    # timestamp_in_seconds = int(match)/1000.0
    # dt = datetime.utcfromtimestamp(timestamp_in_seconds)
    # dt = dt.replace(tzinfo=timezone.utc)

    # # Format the datetime as a string
    # formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S%z')
    # return formatted_date
    return match


def send_file_to_kafka(file_path):
    with open(file_path, 'rb') as data_file:
        crypto_binance_data = json.loads(data_file.read())
        for sym in crypto_binance_data:
            producer.send(kafka_topic, value=sym)
        # file_content = file.read()
        # producer.send(kafka_topic, value=file_content)
        # print(f"File '{file_path}' published to Kafka")


def format_data_to_send(data):
    """
    should only send the following data fields : Timestamp, Open, High, Low, Close, Volume_(SUBJECT), Volume_(Currency), Weighted_Price
    """



if __name__ == "__main__":
    event_handler = FileEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path=directory_to_monitor, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
