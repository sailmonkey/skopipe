import os
import time
import logging
import json # for parsing JSON data from Finnhub
import boto3 # for connecting to FlashBlade using Boto3
import websocket # for connecting to Finnhub Websocket

# Configure Logger
logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s:%(funcName)s()] %(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)

# Variables required for FinnHub configuration
finnhub_api_key = os.environ['FINNHUB_API_KEY']
base_url = 'wss://ws.finnhub.io?'
query = 'token={}'.format(finnhub_api_key)

# Ticker to be used with pretty ticker for file names
ticker = "BINANCE:BTCUSDT"
ticker_pretty = ticker.replace(":", "_")

#FB access
fb_access_key = os.environ['NYC_FB200_ACCESS_KEY']
fb_secret_access_key = os.environ['NYC_FB200_SECRET_ACCESS_KEY']
fb_endpoint = 'http://10.234.112.148'

# Handle message received from Ginnhub websocket
def on_message(ws, message):
     # Load message from FinnHub as JSON format & create filename to be stored as key
    json_data = json.loads(message)
    filename = str(int(time.time())) + '_' + ticker_pretty + '_data.json'
    logger.info('Loaded: %s', filename)

    # Create connection to FB using Boto3 and authenticate using access keys
    s3 = boto3.client(
        's3',
        endpoint_url=fb_endpoint,
        aws_access_key_id=fb_access_key, 
        aws_secret_access_key=fb_secret_access_key
    )

    # Upload FinnHub data to FlashBlade using Boto3
    logger.debug('Putting object into finnhub-ingest')
    s3.put_object(
        Key=filename,
        Body=json.dumps(json_data),
        Bucket='finnhub-ingest'
    )
    logger.info('Object moved to finnhub-ingest: %s', filename)
    logger.debug('FinnHub Message: %s', json.dumps(json_data))

# Additional functions to handle connection to Finnhub
def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"' + ticker + '"}')

if __name__ == "__main__":
    # Uncomment the line below if you need to debug messages received from Finnhub
    #websocket.enableTrace(True)

    # Create connection to Finnhub Webocket and run continuously
    ws = websocket.WebSocketApp(base_url + query,
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close,
                              on_open = on_open)
    ws.run_forever()
