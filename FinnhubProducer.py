import websocket # for connecting to Finnhub Websocket
import json # for parsing JSON data from Finnhub
import os
import time
import boto3 # for connecting to FlashBlade using Boto3


# Variables required for FinnHub configuration
finnhub_api_key = "cngk6vpr01qq9hn92hq0cngk6vpr01qq9hn92hqg"
base_url = 'wss://ws.finnhub.io?'
query = 'token={}'.format(finnhub_api_key)

# Ticker to be used with pretty ticker for file names
ticker = "BINANCE:BTCUSDT"
ticker_pretty = ticker.replace(":", "_")

#FB access
fb_access_key='PSFBSAZQHGDJCEAFGHLPOLCNGBACAEDAODGJNMCF'
fb_secret_access_key='E4E8963E3402016/555c7B2D10A3ed4a863bGFNP'
fb_endpoint = 'http://10.234.112.148'

# Work to be perofrmed once message is received form the FinnHub Websocket
def on_message(ws, message):
    json_data = json.loads(message) # Load message form FinnHub as JSON format
    filename = str(int(time.time())) + '_' + ticker_pretty + '_data.json'

    # Create connection to FB using Boto3 and authenticate using access keys
    s3 = boto3.client(
        's3',
        endpoint_url=fb_endpoint,
        aws_access_key_id=fb_access_key, 
        aws_secret_access_key=fb_secret_access_key
    )

    # Upload FinnHub data to FlashBlade using Boto3
    s3.put_object(
        Key=filename,
        Body=json.dumps(json_data),
        Bucket='datalake'
    )

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"' + ticker + '"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(base_url + query,
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()