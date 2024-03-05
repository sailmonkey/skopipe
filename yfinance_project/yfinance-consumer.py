from confluent_kafka import Consumer, KafkaError
import csv
import os
import socket
from dataclasses import dataclass

csv_file = 'stock_prices.csv' # File to store the stock prices

# Kafka consumer configuration
conf = {'bootstrap.servers': '10.234.112.205:9092',
        'client.id': socket.gethostname(),
        'auto.offset.reset': 'earliest'
        }
# Create a consumer instance
producer = Producer(conf)
print('Kafka Producer has been initiated...')

# Subscribe to the Kafka topic yfinance
topic = 'yfinance'
Consumer.subscribe([topic])

# Consume messages
while True:
 msg = Consumer.poll(1.0)
 
 if msg is None:
   continue
 if msg.error():
   if msg.error().code() == KafkaError._PARTITION_EOF:
     print(‘Reached end of partition’)
   else:
     print(f’Error: {msg.error()}’)
 else:
   print(f’received {msg.value().decode(“utf-8”)}’)
   data_price = [(datetime.now().strftime(“%Y-%m-%d %H:%M:%S”), msg.value().decode(“utf-8”))]
   if not os.path.exists(csv_file):
 # If it doesn’t exist, create a new file and write header and data
     with open(csv_file, mode=”w”, newline=””) as file:
       writer = csv.writer(file)
       writer.writerow([“time”, “price”]) # Header
       writer.writerows(data_price)
   else:
   # If it exists, open it in append mode and add data
     with open(csv_file, mode=”a”, newline=””) as file:
       writer = csv.writer(file)
       writer.writerows(data_price)