import websocket
import json

counter = 0
api_key = "cngk6vpr01qq9hn92hq0cngk6vpr01qq9hn92hqg"

def on_message(ws, message):
    global counter

    data = json.loads(message)

    print(data)
    counter += 1
    print(" ============== Total messages received ================", counter)

    if counter == 10:
        ws.close()

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')

base_url = 'wss://ws.finnhub.io?'

query = 'token={}'.format(api_key)


if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(base_url + query,
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()