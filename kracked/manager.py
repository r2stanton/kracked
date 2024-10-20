
from kracked.feeds import KrakenL1, KrakenL2, KrakenL3, KrakenOHLC, KrakenTrades, KrakenInstruments
from kracked.actions import KrakenExecutions
import threading, queue, toml, time

class KrakenFeedManager:
    def __init__(self, symbols, api_key, api_secret, trace=False, output_directory=".",
                 L1=False, L2=False, L3=False, ohlc=False, trades=False, instruments=False,
                 L1_params={}, L2_params={}, L3_params={}, ohlc_params={}, trades_params={}, instruments_params={}
                 ):
        print("In Constructor")
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbols = symbols

        if type(symbols) == str:
            symbols = [symbols]
        self.feeds = []
        # Parse parameters for each feed.
        if L1:
            print("KrakenFeedManager: Initializing L1 feed")
            self.L1 = KrakenL1(symbols,
                               trace=False,
                               output_directory=output_directory,
                               )
            self.feeds.append(self.L1)

        if L2:
            print("KrakenFeedManager: Initializing L2 feed")
            log_book_every = L2_params.get("log_book_every", 100)
            log_bbo_every = L2_params.get("log_bbo_every", 200)
            depth = L2_params.get("depth", 10)


            self.L2 = KrakenL2(symbols,
                               trace=False,
                               output_directory=output_directory,
                               depth=depth,
                               log_book_every=log_book_every,
                               log_bbo_every=log_bbo_every,
                            )
            self.feeds.append(self.L2)
        if L3:
            print("KrakenFeedManager: Initializing L3 feed")
            log_ticks_every =  L3_params.get("log_ticks_every", 100)
            log_for_webapp = L3_params.get("log_for_webapp", False)
            self.L3 = KrakenL3(symbols,
                            api_key=self.api_key,
                            secret_key=self.api_secret,
                            trace=False,
                            log_ticks_every=log_ticks_every,
                            log_for_webapp=log_for_webapp,
                            output_directory=output_directory)
            self.feeds.append(self.L3)
        if ohlc:
            print("KrakenFeedManager: Initializing OHLC feed")
            interval = ohlc_params.get("interval", 5)
            self.ohlc = KrakenOHLC(symbols,
                                trace=False,
                                output_directory=output_directory,
                                interval=interval,)
            self.feeds.append(self.ohlc)
        if trades:
            print("KrakenFeedManager: Initializing Trades feed")
            log_trades_every = trades_params.get("log_trades_every", 100)
            self.trades = KrakenTrades(symbols,
                                    trace=False,
                                    log_trades_every=log_trades_every,
                                    output_directory=output_directory)
            self.feeds.append(self.trades)
        if instruments:
            print("KrakenFeedManager: Initializing Instruments feed")
            self.instruments = KrakenInstruments(trace=False,
                                                 output_directory=output_directory)
            self.feeds.append(self.instruments)
        # Initialize WebSocket instances
        # Queues for inter-thread communication
        self.message_queue = queue.Queue()

        # Threads list
        self.threads = []

    def start_all(self):
        # Start each WebSocket in its own thread
        for feed in self.feeds:
            thread = threading.Thread(target=feed.launch)
            thread.daemon = True
            thread.start()
            self.threads.append(thread)
            print(f"Started WebSocket thread for {feed.__class__.__name__}")

        # Start a processing thread
        processing_thread = threading.Thread(target=self.process_messages)
        processing_thread.daemon = True
        processing_thread.start()
        self.threads.append(processing_thread)
        print("Started message processing thread.")

    def process_messages(self):
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
        self.executions.stop_websocket()
        self.trades.stop_websocket()
        self.ohlc.stop_websocket()
        self.l2.stop_websocket()

        # Stop processing thread
        self.message_queue.put(None)

        for thread in self.threads:
            thread.join(timeout=5)
        print("All WebSocket threads have been stopped.")

if __name__ == "__main__":
    with open("/home/alg/.api.toml", "r") as fil:
        data = toml.load(fil)
    api_key = data["kraken_api"]
    api_secret = data["kraken_sec"]

    manager = KrakenFeedManager("BTC/USD",
                                api_key=api_key,
                                api_secret=api_secret,
                                L1=False,
                                L2=False,
                                L3=False,
                                ohlc=False,  
                                trades= False,
                                instruments=True,

                                L3_params={"log_ticks_every": 10},
                                ohlc_params={"interval": 1},
                                trades_params={"log_trades_every": 1},
                                )
    manager.start_all()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Interrupted by user. Stopping all WebSockets...")
        manager.stop_all()