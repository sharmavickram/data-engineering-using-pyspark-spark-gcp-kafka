import csv
import json
import time

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC: str = "dataedge"

if __name__ == "__main__":
    print("kafka producer started....")

    kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    message = None

    with open("D:\WORKBOOK\PYSPARK_BOOK\data-engineering-using-pyspark\data\csv\data01.csv", 'r') as file:
        reader = csv.reader(file, delimiter=',')
        for msg in reader:
            message = ",".join(msg)
            print(message)
            kafka_producer.send(KAFKA_TOPIC, json.dumps(message).encode('utf-8'))
            time.sleep(2)
