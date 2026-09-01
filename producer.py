import csv
import time
from kafka import KafkaProducer, errors
import json
import sys

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'stock-stream-topic'

# Step 1: Create Kafka producer with error handling
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5
    )
    # Try fetching metadata to confirm broker availability
    producer.partitions_for(TOPIC)
    print("✅ Connected to Kafka broker at", KAFKA_BROKER)
except errors.NoBrokersAvailable as e:
    print("❌ Kafka broker not available at", KAFKA_BROKER)
    sys.exit(1)
except Exception as e:
    print("❌ Error connecting to Kafka:", str(e))
    sys.exit(1)

# Step 2: Read stock data and send to Kafka
with open('stock_data.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        try:
            producer.send(TOPIC, value=row)
            print(f"Produced: {row}")
            time.sleep(1)
        except Exception as e:
            print("❌ Failed to send message:", e)