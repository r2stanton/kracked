import websocket, json, threading, hashlib, toml
import urllib.parse, hmac, base64, time, requests


class BaseKrakenFeed:

    def get_kraken_signature(self, urlpath, data, secret):
        """
        Base function for getting validation token. Use at your own risk, and 
        feel free to add complexity to the verification process as desired.

        Params:
        =======

        urlpath (str):
            Path for the websocket token validation.
        data (dict):
            Pass the nonce here for security purposes.
        secret (str): 
            Your secret API key.

        Returns:
        ========

        Decoded signature.


        """
        postdata = urllib.parse.urlencode(data)
        encoded = (str(data['nonce']) + postdata).encode()
        message = urlpath.encode() + hashlib.sha256(encoded).digest()
        mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())

        return sigdigest.decode()


    def get_ws_token(self, api_key, api_secret):
        """

        Get the websocket token.

        Params:
        =======

        api_key (str): 
            Your Kraken API key.
        api_secret (str):
            Your Kraken Secret key.

        Returns:
        ========

        Kraken response (e.g. your ws token)

        """
        url = "https://api.kraken.com/0/private/GetWebSocketsToken"
        nonce = int(time.time() * 1000)

        data = { "nonce": nonce, }

        headers = {
            "API-Key": api_key,
            "API-Sign": get_kraken_signature("/0/private/GetWebSocketsToken", data, api_secret)
        }
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()['result']['token']
        else:
            raise Exception(f"Failed to get WebSocket token: {response.text}")


    def on_message(self, ws, message):
        """
        ***THIS IS THE KEY FUNCTION TO OVERWRITE TO EXTEND FUNCTIONALITY***
        
        The base class simply prints the message.
        """
        print(type(message))

    def on_error(self, ws, error):
        """
        """

        print(f"\n================Error====================")
        print(error)
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
        websocket.enableTrace(False)
        conn = "wss://ws.kraken.com/v2"
        conn = "wss://ws-auth.kraken.com/v2"
        ws = websocket.WebSocketApp(conn,
                                    on_open=on_open,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        
        ws.run_forever()

