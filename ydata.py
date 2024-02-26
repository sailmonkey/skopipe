import yfinance as yahooFinance    # Yahoo finance API
import pandas as pd   # Data manipulation library
import duckdb    # DuckDB database

# Define stock symbols and data range for data extraction
symbols = ['PSTG', 'NTAP', 'DELL']
start_date = '2024-02-01'
end_date = '2024-02-26'

# Get stock information for the defined symbols and extract the data from Yahoo Fiance  
data = pd.DataFrame()
for symbol in symbols:
    ticker = yahooFinance.Ticker(symbol)
    stock_data = ticker.history(start=start_date, end=end_date)
    stock_data['symbol'] = symbol
    data = pd.concat([data, stock_data])

# Clean and transform the data
data = data.reset_index()
data = data.drop(['Dividends', 'Stock Splits', 'Volume'], axis=1)
data = data.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'symbol': 'symbol'})

# Test Print the stock information
# print(data)

# Load the data into DuckDB database with context manager
with duckdb.connect("yfinance.db") as con:
    #con.sql("CREATE TABLE stock_data (date DATE, open FLOAT, high FLOAT, low FLOAT, close FLOAT, symbol VARCHAR)")
    con.register('df', data)
    con.sql("INSERT INTO stock_data SELECT * FROM df")
    con.table("stock_data").show()
    # the context manager closes the connection automatically
