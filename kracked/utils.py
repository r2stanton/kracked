

multifeed_lines = """
from kracked.manager import KrakenFeedManager
import threading
import toml, time

# Load your API credentials from a toml file, or set them directly.
# with open("/path/to/.api.toml", "r") as fil:
#     data = toml.load(fil)
# api_key = data['kraken_api']
# api_secret = data['kraken_sec']

api_key = "YOUR_API_KEY"
api_secret = "YOUR_SECRET_KEY"

all_feeds = KrakenFeedManager(
    ["DOGE/USD", "ETH/USD", "BTC/USD", "SOL/USD"],
    api_key,
    api_secret,
    L1=False,
    L2=True,
    L3=True,
    trades=True,
    ohlc=True,
    output_directory=".",

    L2_params={
        "log_book_every": 1,
        "output_mode": "sql",       # "csv", "parquet", or "sql"
    },
    L3_params={
        "log_ticks_every": 1000,
        "output_mode": "parquet",   # "csv", "parquet", or "sql"
    },
    ohlc_params={
        "interval": 5,
        "ccxt_snapshot": True,
        "output_mode": "sql",       # "csv" or "sql"
    },
    trades_params={
        "log_trades_every": 50,
        "output_mode": "parquet",   # "csv", "parquet", or "sql"
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
"""

L1_lines = """
from kracked.feeds import KrakenL1

l1feed = KrakenL1(
    "DOGE/USD",             # List or str of requested symbols.
    output_directory=".",   # Output directory for L1 data files.
    output_mode="csv",      # "csv" or "sql"
    trace=False,            # Do not clog output with WS info.
)

l1feed.launch()
"""

L2_lines = """
from kracked.feeds import KrakenL2

l2feed = KrakenL2(
    ["DOGE/USD", "BTC/USD"],    # List or str of requested symbols.
    log_book_every=1,           # Emit a book snapshot every N updates.
    append_book=True,           # Append book mode, saves historical L2 data.
    output_directory=".",       # Output directory for L2 data files.
    output_mode="parquet",      # "csv", "parquet", or "sql"
    trace=False,                # Do not clog output with WS info.
)

l2feed.launch()                 # Launch the L2 data feed.
"""

L3_lines = """
from kracked.feeds import KrakenL3

# api_key = YOUR_API_KEY
# api_secret = YOUR_SECRET_KEY

l3feed = KrakenL3(
    ["DOGE/USD", "BTC/USD"],    # List or str of requested symbols.
    api_key=api_key,            # Your API key for Kraken.
    secret_key=api_secret,      # Your secret key for Kraken.
    output_directory=".",       # Output directory for L3 data files.
    output_mode="parquet",      # "csv", "parquet", or "sql"
    trace=False,                # Do not clog output with WS info.
)

l3feed.launch()
"""

trades_lines = """
from kracked.feeds import KrakenTrades

tradefeed = KrakenTrades(
    ["BTC/USD", "DOGE/USD"],    # List or str of requested symbols.
    output_directory=".",       # Output directory for trade data files.
    output_mode="parquet",      # "csv", "parquet", or "sql"
    log_trades_every=50,        # Emit a batch after accumulating this many trades.
    trace=False,                # Do not clog output with WS info.
)

tradefeed.launch()
"""

ohlc_lines = """
from kracked.feeds import KrakenOHLC

ohlcfeed = KrakenOHLC(
    ["DOGE/USD"],           # List or str of requested symbols.
    interval=5,             # Interval in minutes.
    output_directory=".",
    output_mode="csv",      # "csv" or "sql"
    trace=False,            # Do not clog output with WS info.
    ccxt_snapshot=True,     # Use the CCXT REST snapshot for deeper history.
)

# BEWARE if you use ccxt_snapshot, you get 720 historical candles, HOWEVER
# they will not contain information about VWAP, TRADES, etc.

# If you choose not to use ccxt for the historical data from Kraken, you will
# only get 5 historical candlesticks.

ohlcfeed.launch()
"""

def get_multifeed_script() -> None:
    """
    Helper function to get example script for multifeed manager.
    """
    with open("multifeed_example_util.py", "w") as fil:
        fil.write(multifeed_lines)

def get_L1_script() -> None:
    """
    Helper function to get example script for L1 feed.
    """
    with open("L1_example_util.py", "w") as fil:
        fil.write(L1_lines) 

def get_L2_script() -> None:
    """
    Helper function to get example script for L2 feed.
    """
    with open("L2_example_util.py", "w") as fil:
        fil.write(L2_lines)

def get_L3_script() -> None:
    """
    Helper function to get example script for L3 feed.
    """
    with open("L3_example_util.py", "w") as fil:
        fil.write(L3_lines)

def get_trades_script() -> None:
    """
    Helper function to get example script for trades feed.
    """
    with open("trade_example_util.py", "w") as fil:
        fil.write(trades_lines)

def get_ohlc_script() -> None:
    """
    Helper function to get example script for OHLC feed.
    """
    with open("OHLC_example_util.py", "w") as fil:
        fil.write(ohlc_lines)

def get_all_scripts() -> None:
    """
    Helper function to get example scripts for all types of feeds.
    """
    get_multifeed_script()
    get_L1_script()
    get_L2_script()
    get_L3_script()
    get_trades_script()
    get_ohlc_script()
