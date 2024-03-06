from confluent_kafka import Producer # Kafka Python client
import yfinance as yahooFinance    # Yahoo finance API
import time  # For the interval between stock price fetches
import requests # For making HTTP requests to the Yahoo finance API
import json # For parsing the JSON response from the Yahoo finance API
import socket # For getting the hostname of the machine running the script

# Kafka producer configuration
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Content-Type': 'application/json',
    'Authorization': 'Bear <token>'
}

# Create a producer instance
conf = {'bootstrap.servers': '10.234.112.205:9092',
        'client.id': socket.gethostname()}
producer = Producer(conf)
print('Kafka Producer has been initiated...')

# Kafka topic to send the stock price data. Created manually using the kafka-topics.sh script
topic = 'yfinance'

# Ticker symbol for stock
tksymbol = 'PSTG'

def fetch_and_send_stock_price():
 while True:
   try:
     url = 'https://query2.finance.yahoo.com/v8/finance/chart/'+tksymbol
     response = requests.get(url, headers=headers)
     data = json.loads(response.text)
     price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
     print(tksymbol, ':', price)
     #Function to fetch stock price and send to Kafka
     # Produce the stock price to the Kafka topic
     producer.produce(topic, key=str(tksymbol), value=str(price))
     print('Now flushing the producer')
     producer.flush()
     print(f"Sent {tksymbol} price to Kafka: {price}")
   except Exception as e:
    print(f"Error fetching/sending stock price: {e}")

# Sleep for a specified interval (e.g., 5 seconds) before fetching the next price
   time.sleep(60)

# Start sending stock price data
fetch_and_send_stock_price()
