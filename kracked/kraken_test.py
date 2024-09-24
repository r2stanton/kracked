import websocket, json, threading, hashlib, toml
import urllib.parse, hmac, base64, time, requests

def get_kraken_signature(urlpath, data, secret):
    # Encode dat for request into URL.
    postdata = urllib.parse.urlencode(data)
    encoded = (str(data['nonce']) + postdata).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    sigdigest = base64.b64encode(mac.digest())
    return sigdigest.decode()

def get_ws_token(api_key, api_secret):
    url = "https://api.kraken.com/0/private/GetWebSocketsToken"
    nonce = int(time.time() * 1000)
    data = {
        "nonce": nonce,
    }
    headers = {
        "API-Key": api_key,
        "API-Sign": get_kraken_signature("/0/private/GetWebSocketsToken", data, api_secret)
    }
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['result']['token']
    else:
        raise Exception(f"Failed to get WebSocket token: {response.text}")


def on_message(ws, message):
    print(f"Received message: {message}")

def on_error(ws, error):
    print(f"\n===========================================")
    print(f"===========Error: {error}=================")
    print(f"===========================================\n")

def on_close(ws, close_status_code, close_msg):
    print(f"\n===========================================")
    print("========WebSocket connection closed======")
    print(f"===========================================\n")
def on_open(ws):
    print("Kraken v2 Connection Opened.")
    
    # Authentication
    with open("/home/alg/.api.toml", "r") as fil:
        data = toml.load(fil)

    api_key = data['kraken_api']
    api_secret = data['kraken_sec']
    ws_token = get_ws_token(api_key, api_secret)
    
    # auth_message = {
        # "event": "subscribe",
        # "subscription": {
            # "name": "ownTrades",
            # "token": ws_token
        # }
    # }
    # ws.send(json.dumps(auth_message))
    
    # Subscribe to a public channel (e.g., ticker for BTC/USD)
    subscription = {
        "method": "subscribe",
        "params": {
            "channel": "level3",
            "symbol": ["BTC/USD"],
            "token": ws_token
        }
    }
    ws.send(json.dumps(subscription))

def run_websocket():
    websocket.enableTrace(True)
    conn = "wss://ws.kraken.com/v2"
    conn = "wss://ws-auth.kraken.com/v2"
    ws = websocket.WebSocketApp(conn,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    
    ws.run_forever()

if __name__ == "__main__":
    websocket_thread = threading.Thread(target=run_websocket)
    websocket_thread.start()
    
    try:
        websocket_thread.join()
    except KeyboardInterrupt:
        print("Exiting...")
