# Script to ingest yahoo finance data into a pandas dataframe. 

import pandas as pd
import numpy as np
import datetime
import pandas_datareader.data as web
import matplotlib.pyplot as plt
from matplotlib import style


def get_data(ticker, start, end):
    df = web.DataReader(ticker, 'yahoo', start, end)
    return df

def main():
    start = datetime.datetime(2010, 1, 1)
    end = datetime.datetime(2015, 1, 1)
    ticker = 'AAPL'
    df = get_data(ticker, start, end)
    print(df.head())
    df['Adj Close'].plot()
    plt.show()

if __name__ == '__main__':
    main()


# End of file
    

    