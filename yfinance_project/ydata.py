import yfinance as yahooFinance    # Yahoo finance API
import pandas as pd   # Data manipulation library
import duckdb    # DuckDB database

# Define stock symbols and data range for data extraction
symbols = ['PSTG']
start_date = '2024-02-01'
end_date = '2024-02-26'

# Get stock information for the defined symbols and extract the data from Yahoo Finance  
data = pd.DataFrame()
for symbol in symbols:
    ticker = yahooFinance.Ticker(symbol)
    stock_data_info = ticker.info
    stock_data['symbol'] = symbol
    print('raw data:', stock_data) #raw data from yahoo finance
    stock_data = ticker.history(start=start_date, end=end_date)
    data = pd.concat([data, stock_data])
    symbol_share_price.reset_index(inplace=True)
    symbol_share_price.plot(x='Date', y='Close', title=symbol)
    print(symbol_share_price)

# Clean and transform the data
data = data.reset_index()   # Reset the index     
data = data.drop(['Dividends', 'Stock Splits', 'Volume'], axis=1)

# Test Print the stock information
print(data)

# Load the data into DuckDB database with context manager
with duckdb.connect("yfinance.db") as con:
    #con.sql("CREATE TABLE stock_data (date DATE, open FLOAT, high FLOAT, low FLOAT, close FLOAT, symbol VARCHAR)")  #One time table creation
    con.register('df', data)
    con.sql("INSERT INTO stock_data SELECT * FROM df")
    con.table("stock_data").show()
    # the context manager closes the connection automatically
