import json
import csv
from collections import defaultdict, deque
from kafka import KafkaConsumer

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "stock-stream-topic"

# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="finance-analytics-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Store previous price for each stock
previous_prices = {}

# Store last 3 prices for moving average
price_history = defaultdict(lambda: deque(maxlen=3))

# Output file
output_file = "processed_stock_data.csv"

# Create output CSV
with open(output_file, "w", newline="") as file:
    writer = csv.writer(file)

    writer.writerow([
        "timestamp",
        "symbol",
        "price",
        "volume",
        "previous_price",
        "price_change",
        "percentage_change",
        "moving_average_3"
    ])

print("==========================================")
print("   FINANCE STREAMING ANALYTICS CONSUMER")
print("==========================================")
print(f"Connected to Kafka topic: {TOPIC}")
print("Waiting for stock data...\n")

try:
    for message in consumer:

        data = message.value

        timestamp = data["timestamp"]
        symbol = data["symbol"]
        price = float(data["price"])
        volume = int(data["volume"])

        # Get previous price for this stock
        previous_price = previous_prices.get(symbol)

        # Calculate price change
        if previous_price is not None:
            price_change = price - previous_price
            percentage_change = (price_change / previous_price) * 100
        else:
            price_change = 0
            percentage_change = 0

        # Add current price to history
        price_history[symbol].append(price)

        # Calculate 3-record moving average
        moving_average = sum(price_history[symbol]) / len(price_history[symbol])

        # Store current price as previous price
        previous_prices[symbol] = price

        # Write processed record
        with open(output_file, "a", newline="") as file:
            writer = csv.writer(file)

            writer.writerow([
                timestamp,
                symbol,
                price,
                volume,
                round(previous_price, 2) if previous_price is not None else "",
                round(price_change, 4),
                round(percentage_change, 4),
                round(moving_average, 4)
            ])

        # Display analytics
        print("------------------------------------------")
        print(f"Timestamp           : {timestamp}")
        print(f"Stock               : {symbol}")
        print(f"Current Price       : {price}")
        print(f"Volume              : {volume}")

        if previous_price is not None:
            print(f"Previous Price      : {previous_price:.2f}")
            print(f"Price Change        : {price_change:+.4f}")
            print(f"Percentage Change   : {percentage_change:+.4f}%")
        else:
            print("Previous Price      : N/A")
            print("Price Change        : N/A")
            print("Percentage Change   : N/A")

        print(f"3-Record Moving Avg : {moving_average:.4f}")

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")

finally:
    consumer.close()
    print("\nKafka consumer closed.")