import websocket, json, threading, hashlib, toml
import urllib.parse, hmac, base64, time, requests
import asyncio


class BaseKrakenWS:

    def __init__(self, auth=True, trace=False, api_key=None, secret_key=None):
        self.auth = auth
        self.trace = trace
        self.api_key = api_key
        self.secret_key = secret_key

    def launch(self):
        """
        Starts the websocket thread. For the BaseKrakenWS, this simply opens
        an authenticated (e.g. you need to provide API/Secret) L3 data stream
        for BTC/USD.
        """

        websocket_thread = threading.Thread(target=self.run_websocket)
        websocket_thread.start()

        try:
            websocket_thread.join()

        except KeyboardInterrupt:
            print("Exiting...")

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
        encoded = (str(data["nonce"]) + postdata).encode()
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

        data = {
            "nonce": nonce,
        }

        headers = {
            "API-Key": api_key,
            "API-Sign": self.get_kraken_signature(
                "/0/private/GetWebSocketsToken", data, api_secret
            ),
        }

        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["result"]["token"]
        else:
            raise Exception(f"Failed to get WebSocket token: {response.text}")

    def _on_message(self, ws, message):
        """
        ***THIS IS THE KEY FUNCTION TO OVERWRITE TO EXTEND FUNCTIONALITY***

        The base class simply prints the message. These functions are not
        intended to be called, they are passed to the WebSocketApp.

        Params:
        =======
        ws:
            The websocket.
        message:
            Received message.
        """
        print(message)

    def _on_error(self, ws, error):
        """
        Error message for Kraken websocket errors.

        You may want to change this to log to some file.
        """

        print(f"\n================Error====================")
        print(error)
        print(f"===========================================\n")

    def _on_close(self, ws, close_status_code, close_msg):
        """
        Close message for Kraken connection.
        """
        print(f"\n===========================================")
        print("========WebSocket connection closed======")
        print(f"===========================================\n")

    def _on_open(self, ws):
        """
        Open message for Kraken connection.
        """

        print("Kraken v2 Connection Opened.")

        api_key = self.api_key
        api_secret = self.secret_key
        ws_token = self.get_ws_token(api_key, api_secret)

        # DUMMY SUBSCRIPTION EXAMPLE. REUQUIRES AUTH.
        subscription = {
            "method": "subscribe",
            "params": {"channel": "level3", "symbol": ["BTC/USD"], "token": ws_token},
        }
        ws.send(json.dumps(subscription))

    def run_websocket(self):
        websocket.enableTrace(self.trace)

        if self.auth:
            conn = "wss://ws-auth.kraken.com/v2"
        else:
            conn = "wss://ws.kraken.com/v2"

        ws = websocket.WebSocketApp(
            conn,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        ws.run_forever()


if __name__ == "__main__":
    with open(f"/home/alg/.api.toml", "r") as fil:
        data = toml.load(fil)
    api_key = data["kraken_api"]
    api_secret = data["kraken_sec"]

    myws = BaseKrakenWS(auth=True, trace=False, api_key=api_key, secret_key=api_secret)

    myws.launch()
