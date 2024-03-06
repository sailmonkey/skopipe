# App to load data into Cassandra Data Base from Data Lake
import boto3
import json
import os
import uuid # for generating unique ID for each trade
import time
from cassandra.cluster import Cluster

#static to be replaced with json data
symbol = "BINANCE:BTCUSDT"
trade_timestamp = time.time()
ingest_timestamp = time.time()
price = 45002
trade_conditions = "T"
volume = 1
id = uuid.uuid4() # Generate a unique ID for each trade

def main():
    # Connect to Cassandra
    global cluster
    cluster = Cluster(['10.234.112.101'])
    session = cluster.connect('market')
    print("Connected to Cassandra")
    generateInsertData(symbol, trade_timestamp, ingest_timestamp, price, trade_conditions, id, volume, session)

def generateInsertData(symbol, trade_timestamp, ingest_timestamp, price, trade_conditions, id, volume, session):
    insert_query = session.prepare("\
                INSERT INTO trades (symbol, trade_timestamp, ingest_timestamp, price, trade_conditions, uuid, volume)\
                VALUES (?, ?, ?, ?, ?, ?, ?)\
                IF NOT EXISTS\
                ")
    number_of_rows = 3 # Number of rows to be inserted into Cassandra
    for t in range(0, number_of_rows):
        trade_timestamp = time.time() # Generate a new timestamp for each trade becuase its a primary key
        time.sleep(2)
        try:
            session.execute(insert_query, [symbol, trade_timestamp, ingest_timestamp, price, trade_conditions, id, volume])
            print("Data Inserted into Cassandra Successfully")
        except Exception as e: 
            print(e)
    # Query the data from Cassandra to verify the data has been inserted
    rows = session.execute('SELECT symbol, price FROM trades')
    for symbol in rows:
        print(symbol.symbol, symbol.price)
   
    session.shutdown(); # Close the session
    cluster.shutdown(); # Close the connection to Cassandra
    print("Closed Connection to Cassandra")

if __name__ == "__main__":
    main()