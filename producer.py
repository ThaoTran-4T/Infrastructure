from kafka import KafkaProducer
from kafka.errors import KafkaError
import pandas as pd
import json
import time
from dotenv import load_dotenv
import os
load_dotenv()

secret = os.getenv('secret')

timing = 3
# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['dp-event-hub.servicebus.windows.net:9093'],
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='$ConnectionString',
    sasl_plain_password=secret,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
topic = 'dp-event-hub'
# Load data from CSV

def send_to_kafka(row):
    try:
        # Convert row to dictionary, then to JSON
        message = row.to_dict()
        # Send to Kafka topic 'transaction-log'
        future = producer.send(topic, value=message)
        # Confirm successful delivery
        record_metadata = future.get(timeout=10)
        print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition}")
    except KafkaError as e:
        print(f"Failed to send message: {e}")
        


df = pd.read_csv('Data/points_transaction_log.csv')

# Iterate through the DataFrame rows and send each row to Kafka
for index, row in df.iterrows():
    send_to_kafka(row)
    time.sleep(timing)
