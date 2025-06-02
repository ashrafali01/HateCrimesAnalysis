#This is kafka producer which will ingest data in kafka topic 
import pandas as pd
from confluent_kafka import Producer
import json
import time

# Kafka configuration (use your actual Confluent credentials)
conf = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'YourAccessKey',
    'sasl.password': 'YourSecretKey',
}

producer = Producer(conf)
topic = 'hatecrimesconsumer'  # topic name

# Read CSV
df = pd.read_csv('YourCSVPath')
df.fillna('', inplace=True)  # Replace NaNs

# Produce each row as JSON
for index, row in df.iterrows():
    message = row.to_json()
    producer.produce(topic, key=str(index), value=message)
    producer.poll(0)
    time.sleep(0.1)  # optional delay

producer.flush()
print("All messages sent to Kafka.")
