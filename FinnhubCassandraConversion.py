import pandas as pd


def convert_df(df):
    df = rename_columns(df)
    df = extract_exchange_name(df)
    return df

def rename_columns(df):
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
    df[['exchange','ticker']] = df['symbol'].str.split(':',expand=True)
    return df
