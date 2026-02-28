from kracked.manager import KrakenFeedManager
import threading
import toml, time

# Load your API credentials from a toml file, or set them directly.
api_key = "YOUR_API_KEY"
api_secret = "YOUR_SECRET_KEY"

all_feeds = KrakenFeedManager(
    ["DOGE/USD", "BTC/USD", "SOL/USD", "ETH/USD"],
    api_key,
    api_secret,
    L1=False,
    L2=True,
    L3=False,
    trades=True,
    ohlc=True,
    output_directory="ex_multifeed_out",

    L2_params={
        "log_book_every": 500,
        "output_mode": "sql",       # "csv", "parquet", or "sql"
    },
    L3_params={
        "log_ticks_every": 1000,
        "output_mode": "sql",   # "csv", "parquet", or "sql"
    },
    ohlc_params={
        "interval": 5,
        "ccxt_snapshot": True,
        "output_mode": "sql",       # "csv" or "sql"
    },
    trades_params={
        "log_trades_every": 50,
        "output_mode": "sql",   # "csv", "parquet", or "sql"
    },
)

# BEWARE if you use ccxt_snapshot, you get 720 historical candles, HOWEVER
# they will not contain information about VWAP, TRADES, etc.

# If you choose not to use ccxt for the historical data from Kraken, you will
# only get 5 historical candlesticks.

all_feeds.start_all()

try:
    # Checks the status of all feed threads and restarts any that have died.
    while True:
        time.sleep(1)

        for name, thread in list(all_feeds.threads.items()):
            if not thread.is_alive():
                print(f"{name} thread died, restarting...")
                curr_feed = all_feeds.feeds[name]
                restarted_thread = threading.Thread(target=curr_feed.launch)
                restarted_thread.daemon = True
                restarted_thread.start()
                all_feeds.threads[name] = restarted_thread

except KeyboardInterrupt:
    print("Shutting down feeds...")
    all_feeds.stop_all()
