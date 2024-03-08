import pandas as pd
import logging

# Configure Logger
logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s:%(funcName)s()] %(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)

def convert_df(df):
    # Run all of the required conversion steps to prepare data for Cassandra insertion
    logger.info('Running conversion service')
    df = rename_columns(df)
    df = extract_exchange_name(df)
    return df

def rename_columns(df):
    # Raname columns to match schema for Cassandra, not required but helps with readability and maintanability
    logger.info('Renaming columns')
    df.rename(columns=
              {'c':'trade_conditions', 
               'p':'price', 
               's':'symbol', 
               't':'trade_timestamp', 
               'v':'volume'},
               inplace=True
               )
    return df

def extract_exchange_name(df):
    # Extrat exchange and ticker information form the Symbol field and save as new columns
    logger.info('Extracting exchange name and ticker')
    df[['exchange','ticker']] = df['symbol'].str.split(':',expand=True)
    return df
