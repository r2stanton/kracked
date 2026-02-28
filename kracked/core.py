import websocket, json, threading, hashlib, toml
import urllib.parse, hmac, base64, time, requests
import queue as _queue

from kracked.io import KrackedWriter


class BaseKrakenWS:

    output_queue = None
    _standalone_writer = None
    _standalone_writer_thread = None

    def __init__(self, auth=True, trace=False, api_key=None, secret_key=None):
        self.auth = auth
        self.trace = trace
        self.api_key = api_key
        self.secret_key = secret_key
        self.ws = None

    def _get_writer_config(self):
        """
        Return a dict of kwargs for constructing a KrackedWriter.
        Subclasses populate attributes like output_mode, output_directory, etc.
        that this method reads. Falls back to sensible defaults.
        """
        return {
            "output_directory": getattr(self, "output_directory", "."),
            "output_mode": getattr(self, "output_mode", "sql"),
            "db_name": getattr(self, "db_name", "kracked_outputs.db"),
            "convert_to_parquet_every": getattr(self, "convert_to_parquet_every", 1000),
        }

    def launch(self):
        """
        Starts the websocket thread. If no output_queue has been injected
        (i.e. the feed is running standalone, not through KrakenFeedManager),
        a KrackedWriter and its consumer thread are automatically created so
        that all I/O works out of the box.

        For the BaseKrakenWS, this simply opens an authenticated (e.g. you
        need to provide API/Secret) L3 data stream for BTC/USD.
        """

        if self.output_queue is None:
            self.output_queue = _queue.Queue()
            writer_cfg = self._get_writer_config()
            self._standalone_writer = KrackedWriter(
                output_queue=self.output_queue,
                **writer_cfg,
            )
            self._standalone_writer_thread = threading.Thread(
                target=self._standalone_writer.run,
            )
            self._standalone_writer_thread.daemon = True
            self._standalone_writer_thread.start()

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

        self.ws = websocket.WebSocketApp(
            conn,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        self.is_running = True
        self.ws.run_forever()
        self.is_running = False

    def stop_websocket(self):
        if self.ws is not None:
            self.ws.close()

        if self._standalone_writer is not None:
            self._standalone_writer.stop()
            self._standalone_writer_thread.join(timeout=5)
            self._standalone_writer = None
            self._standalone_writer_thread = None
