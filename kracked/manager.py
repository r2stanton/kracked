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
        monitor_reconnects=True,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbols = symbols
        self.output_directory = output_directory
        self.db_name = db_name
        self.monitor_reconnects = monitor_reconnects

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
            self._configure_feed(self.L1, "L1")
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
            self._configure_feed(self.L2, "L2")
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
            self._configure_feed(self.L3, "L3")
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
            self._configure_feed(self.ohlc, "ohlc")
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
            self._configure_feed(self.trades, "trades")
            self.feeds["trades"] = self.trades
        if instruments:
            print("KrakenFeedManager: Initializing Instruments feed")
            self.instruments = KrakenInstruments(
                trace=False, output_directory=output_directory
            )
            self._configure_feed(self.instruments, "instruments")
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
        self._monitor_thread = None
        self._stop_monitor = False

    def _configure_feed(self, feed, feed_name):
        """Wire a feed into the shared writer and enable connection logging."""
        feed.output_queue = self.output_queue
        feed.feed_name = feed_name
        feed.log_connections = True

    def _monitor_feed_threads(self):
        """Restart feed threads that died from an unexpected disconnect."""
        while not self._stop_monitor:
            time.sleep(1)
            for name, thread in list(self.threads.items()):
                if thread.is_alive():
                    continue
                feed = self.feeds[name]
                if feed._intentional_stop:
                    continue
                print(f"{name} thread died, restarting...")
                restarted_thread = threading.Thread(target=feed.launch)
                restarted_thread.daemon = True
                restarted_thread.start()
                self.threads[name] = restarted_thread

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

        if self.monitor_reconnects:
            self._stop_monitor = False
            self._monitor_thread = threading.Thread(target=self._monitor_feed_threads)
            self._monitor_thread.daemon = True
            self._monitor_thread.start()
            print("Started connection monitor thread.")

    def stop_all(self):
        self._stop_monitor = True
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=2)

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
