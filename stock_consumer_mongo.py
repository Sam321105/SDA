from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from datetime import datetime

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'stock_topic',  # Replace with your Kafka topic name
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='stock-consumer-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb+srv://321105:2001@sda2.ryhnp.mongodb.net/')
db = mongo_client['stock']  # Replace with your database name
stocks_collection = db['amazon']  # Replace with your collection name

# Consumer processing loop
print("Kafka Consumer Started...")
for message in consumer:
    data = message.value

    # Extract stock data fields
    date = data['date']
    open_price = data['open']
    high_price = data['high']
    low_price = data['low']
    close_price = data['close']
    volume = data['volume']

    # Insert stock data into MongoDB
    stocks_collection.insert_one({
        'date': datetime.strptime(date, '%Y-%m-%d'),  # Convert to datetime
        'open': open_price,
        'high': high_price,
        'low': low_price,
        'close': close_price,
        'volume': volume
    })

    # Print alerts or log messages for specific conditions
    if close_price > high_price * 1.05:
        print(f"Alert! Significant price increase on {date}: Close = {close_price}, High = {high_price}")
    elif close_price < low_price * 0.95:
        print(f"Alert! Significant price drop on {date}: Close = {close_price}, Low = {low_price}")
    elif volume > 1_000_000:
        print(f"Alert! High trading volume on {date}: Volume = {volume}")
    else:
        print(f"Data processed for date {date}: Close = {close_price}, Volume = {volume}")

print("Kafka Consumer Finished.")
