# App to load data into Cassandra Data Base from Data Lake
import boto3
import json
import os
import uuid # for generating unique ID for each trade
import datetime
import time
from cassandra.cluster import Cluster


server = '10.234.112.101'
db = 'market'

def get_cluster(server):
    return Cluster([server])

def get_session(cluster, db):
    return cluster.connect(db)

def load_df(df):
    #TODO Create function to load finnhub df into cassandra
    cluster = get_cluster(server)
    session = get_session(cluster, db)
    insert_query = session.prepare("\
            INSERT INTO trades (symbol, trade_timestamp, ingest_timestamp, price, trade_conditions, uuid, exchange, ticker, volume)\
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)\
            IF NOT EXISTS\
            ")
    for index, row in df.iterrows():
        session.execute(
            insert_query, 
            (row['symbol'],
             row['trade_timestamp'],
             row['ingest_timestamp'],
             row['price'],
             row['trade_conditions'],
             row['uuid'],
             row['exchange'],
             row['ticker'],
             row['volume']
             )
        )
    close(session, cluster)
    return None

def generateInsertData(symbol, trade_timestamp, ingest_timestamp, price, trade_conditions, id, volume, session):
    insert_query = session.prepare("\
                INSERT INTO trades (symbol, trade_timestamp, ingest_timestamp, price, trade_conditions, uuid, volume)\
                VALUES (?, ?, ?, ?, ?, ?, ?)\
                IF NOT EXISTS\
                ")
    try:
        session.execute(insert_query, [symbol, trade_timestamp, ingest_timestamp, price, trade_conditions, id, volume])
        print("Data Inserted into Cassandra Successfully" + id)
    except Exception as e: 
        print(e)

def close(session, cluster):
    session.shutdown(); # Close the session
    cluster.shutdown(); # Close the connection to Cassandra
    print("Closed Connection to Cassandra")
