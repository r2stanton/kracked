from kracked.feeds import (
    KrakenL1,
    KrakenL2,
    KrakenL3,
    KrakenOHLC,
    KrakenTrades,
    KrakenInstruments,
)
import threading, queue, toml, time


class KrakenFeedManager:
    def __init__(
        self,
        symbols,
        api_key,
        api_secret,
        trace=False,
        output_directory=".",
        L1=False,
        L2=False,
        L3=False,
        ohlc=False,
        trades=False,
        instruments=False,
        L1_params={},
        L2_params={},
        L3_params={},
        ohlc_params={},
        trades_params={},
        instruments_params={},
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbols = symbols

        self.L1 = None
        self.L2 = None
        self.L3 = None
        self.ohlc = None
        self.trades = None
        self.instruments = None

        if type(symbols) == str:
            symbols = [symbols]
        self.feeds = {}
        # Parse parameters for each feed.
        if L1:
            print("KrakenFeedManager: Initializing L1 feed")
            self.L1 = KrakenL1(
                symbols,
                trace=False,
                output_directory=output_directory,
            )
            self.feeds["L1"] = self.L1
        if L2:
            print("KrakenFeedManager: Initializing L2 feed")
            log_book_every = L2_params.get("log_book_every", 100)
            append_book = L2_params.get("append_book", True)
            depth = L2_params.get("depth", 10)

            self.L2 = KrakenL2(
                symbols,
                trace=False,
                output_directory=output_directory,
                depth=depth,
                log_book_every=log_book_every,
                append_book=append_book,
            )
            self.feeds["L2"] = self.L2
        if L3:
            print("KrakenFeedManager: Initializing L3 feed")
            log_ticks_every = L3_params.get("log_ticks_every", 100)
            log_for_webapp = L3_params.get("log_for_webapp", False)
            self.L3 = KrakenL3(
                symbols,
                api_key=self.api_key,
                secret_key=self.api_secret,
                trace=False,
                log_ticks_every=log_ticks_every,
                log_for_webapp=log_for_webapp,
                output_directory=output_directory,
            )
            self.feeds["L3"] = self.L3
        if ohlc:
            print("KrakenFeedManager: Initializing OHLC feed")
            interval = ohlc_params.get("interval", 5)
            ccxt_snapshot = ohlc_params.get("ccxt_snapshot", False)
            self.ohlc = KrakenOHLC(
                symbols,
                trace=False,
                output_directory=output_directory,
                interval=interval,
                ccxt_snapshot=ccxt_snapshot,
            )
            self.feeds["ohlc"] = self.ohlc
        if trades:
            print("KrakenFeedManager: Initializing Trades feed")
            log_trades_every = trades_params.get("log_trades_every", 100)
            self.trades = KrakenTrades(
                symbols,
                trace=False,
                log_trades_every=log_trades_every,
                output_directory=output_directory,
            )
            self.feeds["trades"] = self.trades
        if instruments:
            print("KrakenFeedManager: Initializing Instruments feed")
            self.instruments = KrakenInstruments(
                trace=False, output_directory=output_directory
            )
            self.feeds["instruments"] = self.instruments

        # Initialize WebSocket instances
        # Queues for inter-thread communication
        self.message_queue = queue.Queue()

        # Threads list
        self.threads = {}

    def start_all(self):
        # Start each WebSocket in its own thread
        for name, feed in zip(self.feeds.keys(), self.feeds.values()):
            print(feed)
            thread = threading.Thread(target=feed.launch)
            thread.daemon = True
            thread.start()
            self.threads[name] = thread
            print(f"Started WebSocket thread for {feed.__class__.__name__}")

        # Start a processing thread
        # processing_thread = threading.Thread(target=self.process_messages)
        # processing_thread.daemon = True
        # processing_thread.start()
        # self.threads.append(processing_thread)
        # print("Started message processing thread.")

    def process_messages(self):
        print("processMessagesCalled")
        while True:
            message = self.message_queue.get()
            if message is None:
                break
            # Process the message
            self.handle_message(message)
            self.message_queue.task_done()

    def handle_message(self, message):
        # Implement your message handling logic here
        print(f"Processing message: {message}")
        # Example: Update internal states, trigger actions, etc.

    def stop_all(self):
        # Implement stopping logic for all WebSockets
        for feed in self.feeds:
            feed.stop_websocket()

        # Stop processing thread
        self.message_queue.put(None)

        for thread in self.threads:
            thread.join(timeout=5)
        print("All WebSocket threads have been stopped.")
