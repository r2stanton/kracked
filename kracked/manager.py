from kracked.feeds import (
    KrakenL1,
    KrakenL2,
    KrakenL3,
    KrakenOHLC,
    KrakenTrades,
    KrakenInstruments,
)
from kracked.io import KrackedWriter
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
        db_name="kracked_outputs.db",
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbols = symbols
        self.output_directory = output_directory

        self.L1 = None
        self.L2 = None
        self.L3 = None
        self.ohlc = None
        self.trades = None
        self.instruments = None

        if type(symbols) == str:
            symbols = [symbols]

        # Shared queue: all feeds push payloads here, one writer consumes.
        self.output_queue = queue.Queue()

        # Collect per-channel output mode overrides for the writer.
        channel_modes = {}
        convert_to_parquet_every = 1000

        self.feeds = {}
        # Parse parameters for each feed.
        if L1:
            print("KrakenFeedManager: Initializing L1 feed")
            output_mode = L1_params.get("output_mode", "sql")
            channel_modes["L1"] = output_mode
            self.L1 = KrakenL1(
                symbols,
                trace=False,
                output_directory=output_directory,
                output_mode=output_mode,
            )
            self.L1.output_queue = self.output_queue
            self.feeds["L1"] = self.L1
        if L2:
            print("KrakenFeedManager: Initializing L2 feed")
            log_book_every = L2_params.get("log_book_every", 100)
            append_book = L2_params.get("append_book", True)
            depth = L2_params.get("depth", 10)
            output_mode = L2_params.get("output_mode", "sql")
            convert_to_parquet_every = L2_params.get("convert_to_parquet_every", 1000)
            channel_modes["L2"] = output_mode

            self.L2 = KrakenL2(
                symbols,
                trace=False,
                output_directory=output_directory,
                depth=depth,
                log_book_every=log_book_every,
                append_book=append_book,
                output_mode=output_mode,
            )
            self.L2.output_queue = self.output_queue
            self.feeds["L2"] = self.L2
        if L3:
            print("KrakenFeedManager: Initializing L3 feed")
            log_ticks_every = L3_params.get("log_ticks_every", 100)
            output_mode = L3_params.get("output_mode", "sql")
            channel_modes["L3"] = output_mode
            self.L3 = KrakenL3(
                symbols,
                api_key=self.api_key,
                secret_key=self.api_secret,
                trace=False,
                log_ticks_every=log_ticks_every,
                output_directory=output_directory,
                output_mode=output_mode
            )
            self.L3.output_queue = self.output_queue
            self.feeds["L3"] = self.L3
        if ohlc:
            print("KrakenFeedManager: Initializing OHLC feed")
            interval = ohlc_params.get("interval", 5)
            ccxt_snapshot = ohlc_params.get("ccxt_snapshot", False)
            output_mode = ohlc_params.get("output_mode", "sql")
            channel_modes["OHLC"] = output_mode
            self.ohlc = KrakenOHLC(
                symbols,
                trace=False,
                output_directory=output_directory,
                interval=interval,
                ccxt_snapshot=ccxt_snapshot,
                output_mode=output_mode
            )
            self.ohlc.output_queue = self.output_queue
            self.feeds["ohlc"] = self.ohlc
        if trades:
            print("KrakenFeedManager: Initializing Trades feed")
            log_trades_every = trades_params.get("log_trades_every", 100)
            output_mode = trades_params.get("output_mode", "sql")
            channel_modes["trades"] = output_mode
            self.trades = KrakenTrades(
                symbols,
                trace=False,
                log_trades_every=log_trades_every,
                output_directory=output_directory,
                output_mode=output_mode
            )
            self.trades.output_queue = self.output_queue
            self.feeds["trades"] = self.trades
        if instruments:
            print("KrakenFeedManager: Initializing Instruments feed")
            self.instruments = KrakenInstruments(
                trace=False, output_directory=output_directory
            )
            self.instruments.output_queue = self.output_queue
            self.feeds["instruments"] = self.instruments

        # Single writer thread for all I/O.
        self.writer = KrackedWriter(
            output_queue=self.output_queue,
            output_directory=output_directory,
            db_name=db_name,
            channel_modes=channel_modes,
            convert_to_parquet_every=convert_to_parquet_every,
        )

        # Threads list
        self.threads = {}
        self._writer_thread = None

    def start_all(self):
        # Start the single writer thread.
        self._writer_thread = threading.Thread(target=self.writer.run)
        self._writer_thread.daemon = True
        self._writer_thread.start()
        print("Started I/O writer thread.")

        # Start each WebSocket in its own thread
        for name, feed in zip(self.feeds.keys(), self.feeds.values()):
            thread = threading.Thread(target=feed.launch)
            thread.daemon = True
            thread.start()
            self.threads[name] = thread
            print(f"Started WebSocket thread for {feed.__class__.__name__}")

    def stop_all(self):
        # Stop all feed websockets.
        for name, feed in self.feeds.items():
            feed.stop_websocket()

        # Signal the writer to flush and exit.
        self.writer.stop()
        if self._writer_thread is not None:
            self._writer_thread.join(timeout=5)

        for name, thread in self.threads.items():
            thread.join(timeout=5)
        print("All WebSocket threads have been stopped.")
