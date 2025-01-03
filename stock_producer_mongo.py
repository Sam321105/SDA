import pandas as pd
import time
from kafka import KafkaProducer
import json
from pymongo import MongoClient



def read_live_match_update_from_csv(file_path):
    return pd.read_csv(file_path)

def send_to_kafka(producer, topic, message):
    producer.send(topic, value=json.dumps(message).encode('utf-8'))
    producer.flush()
    print("Test")

def insert_into_mongo(client,collection,message):
    collection.insert_one(message)

def main():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    live_match_update = read_live_match_update_from_csv('amazon_stock_data.csv')
    client = MongoClient("mongodb+srv://321105:2001@sda2.ryhnp.mongodb.net/")  # replace with your MongoDB connection string
    db = client['stock']  # replace with your database name
    collection = db['ticker_stock_producer']  # replace with your collection name

    print("Main Started")
    for index, row in live_match_update.iterrows():
        message = {
            'Date': row['Date'],
            'Adj Close': row['Adj Close'],
            'Close': row['Close'],
            'High': row['High'],
            'Low': row['Low'],
            'Open': row['Open'],
            'Volume': row['Volume'],
        }
        print("Iteration Started:",message)

        send_to_kafka(producer, 'stock_topic', message)
        insert_into_mongo(client,collection,message)
        time.sleep(1)  # Adjust the sleep time as needed

if __name__ == "__main__":
    main()