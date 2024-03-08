# App to load data into Cassandra Data Base from Data Lake
import boto3
import json
import os
import uuid # for generating unique ID for each trade
import datetime
import time
from cassandra.cluster import Cluster
import logging

# Configure Logger
logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s:%(funcName)s()] %(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)
logging.getLogger('cassandra').setLevel(logging.ERROR)

# Cassandra Server details for connectoin
server = '10.234.112.101'
db = 'market'

def get_cluster(server):
    return Cluster([server])

def get_session(cluster, db):
    return cluster.connect(db)

def load_df(df):
    # Iterate through the DataFrame and insert each record into Cassandra
    logger.info('Loading DataFrame into Cassandra')
    cluster = get_cluster(server)
    session = get_session(cluster, db)

    # Prepare a CQL insert script for the iteration
    insert_query = session.prepare("\
            INSERT INTO trades (symbol, trade_timestamp, ingest_timestamp, price, trade_conditions, uuid, exchange, ticker, volume)\
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)\
            IF NOT EXISTS\
            ")
    
    # Iterate through each row of the DataFrame and execute prepared CQL insert statement
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
    logger.info('Load Complete')
    close(session, cluster)
    return None

def close(session, cluster):
    session.shutdown(); # Close the session
    cluster.shutdown(); # Close the connection to Cassandra
    logger.info('Closed Connection to Cassandra')
