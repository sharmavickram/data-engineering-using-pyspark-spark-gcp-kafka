import json
import time

from kafka import KafkaConsumer

KAFKA_TOPIC = "input"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

if __name__ == "__main__":
    print("kafka consumer started....")

    print("Reading message from kafka topic... ")
    consumer = KafkaConsumer("input", bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    print("consumer created....")
    # Read data from kafka
    for message in consumer:
        print("Consumer records:\n")
    print(message)
