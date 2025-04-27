from kracked.feeds import KrakenL1, KrakenL2, KrakenL3
from kracked.feeds import KrakenTrades, KrakenOHLC

import sqlite3

import warnings
warnings.filterwarnings("ignore")

import threading
import toml
import inspect
import time
import os

# You need to fill these if you want to run the L3 tests.
# api_key = YOUR_API_KEY
# api_secret = YOUR_SECRET_KEY

# TODO: Use Fixtures for this.
if os.path.exists("data"):
    os.system("rm -r data/*")
    os.rmdir("data")
    os.mkdir("data")

def test_sql_trades():
    """
    Tests the SQL output mode of the Trades feed.
    """

    # Instance the KrakenTrades object.
    tradefeed = KrakenTrades(
        ["BTC/USD", "DOGE/USD", "ETH/USD"],    # List or str of requested symbols.
        output_directory="data",  # Output directory for L1 data files.
        # trace=False,                # Do not clog output with WS info.
        # log_trades_every=1,         # How often 
        log_trades_every=4,
        output_mode="sql",
    )

    # Start a background process with the KrakenTrades object.
    thread = threading.Thread(target=tradefeed.launch)
    thread.daemon = True
    thread.start()

    # Give it enough time to pull some data/store in the SQL database.
    time.sleep(3)

    # Stop the background process.
    tradefeed.stop_websocket()

    # Connect to the database and grab a cursor.
    conn = sqlite3.connect("data/kracked_outputs.db")
    cur = conn.cursor()

    # Check the table exists.
    res = cur.execute("SELECT name from sqlite_master where type='table'")
    table_names = [v[0] for v in res.fetchall()]
    assert "trades" in table_names , "trades table not created"

    # Check the table contains all the proper columns.
    res = cur.execute("PRAGMA table_info(trades)")
    column_names = [r[1] for r in res.fetchall()]

    assert column_names == ['ts_event', 'symbol', 'price', 'qty', 'side', 'ord_type', 'trade_id'] , "Columns invalid in trade table"


def test_ohlc_sql():
    """
    Tests the SQL output mode for the OHLC candlesticks.
    """

    # Spin up the KrakenOHLC object.
    ohlcfeed = KrakenOHLC(
        ["DOGE/USD"],               # List or str of requested symbols.
        interval=1,                 # Interval in minutes.
        output_directory="data",    # Output directory for L1 data files.
        ccxt_snapshot=True,         # Use the ccxt snapshot (you get more data)
        output_mode="sql",
    )

    # Start up a daemon process for the websocket/CCXT snapshot.    
    thread = threading.Thread(target=ohlcfeed.launch)
    thread.daemon = True
    thread.start()

    # Give it enough time to pull data.
    time.sleep(5)

    # Stop the background process
    ohlcfeed.stop_websocket()

    # Connect to the database and grab a cursor.
    conn = sqlite3.connect("data/kracked_outputs.db")
    cur = conn.cursor()

    # Check the table xists.
    res = cur.execute("SELECT name from sqlite_master where type='table'")
    table_names = [v[0] for v in res.fetchall()]
    assert "OHLC" in table_names, "OHLC table not created"

    # Check the table contains all the proper columns
    res = cur.execute("PRAGMA table_info(OHLC)")
    column_names = [r[1] for r in res.fetchall()]
    assert column_names == ['timestamp', 'symbol', 'open',
                            'high', 'low', 'close',
                            'volume', 'vwap', 'trades',
                            'tstart', 'ttrue'] , "Columns in OHLC table are not correct."






# L3 needs API auth, so let's just not include this in the test suite (except for those who are
# motivated enough to modify this file and implement :p)
# def test_sql_L3():
#     l3feed = KrakenL3(
#         ["DOGE/USD", "BTC/USD"],        # List or str of requested symbols.
#         api_key=api_key,                # Your API key for Kraken.
#         secret_key=api_secret,          # Your Secrete key for Kraken.
#         output_directory='data',          # Output directory for L3 data files.
#         trace=False,                    # Do not clog output with WS info.
#         output_mode="sql",
#     )
#     l3feed.launch()

def test_sql_L1():
    """
    Tests the SQL output mode for the L1 trades.
    """

    # Spin up the KrakenTrades object.
    l1feed = KrakenL1(
        ["DOGE/USD", "BTC/USD"],    # List or str of requested symbols.
        output_directory="data",  # Output directory for L1 data files.
        output_mode="sql",
    )


    # Start up a daemon process for the websocket/CCXT snapshot.    
    thread = threading.Thread(target=l1feed.launch)
    thread.daemon=True
    thread.start()

    # Give it enough time to pull some data/store in the SQL database.
    time.sleep(5)

    # Stop the background process.
    l1feed.stop_websocket()

    # Connect to the database and grab a cursor.
    conn = sqlite3.connect("data/kracked_outputs.db")
    cur = conn.cursor()

    # Check the table exists.
    res = cur.execute("SELECT name from sqlite_master where type='table'")
    table_names = [v[0] for v in res.fetchall()]
    assert "L1" in table_names, "L1 table not created"

    # Check the table contains all the proper columns.
    res = cur.execute("PRAGMA table_info(L1)")
    column_names = [r[1] for r in res.fetchall()]

    assert column_names == ['timestamp', 'symbol', 'bid',
                            'bid_qty', 'ask', 'ask_qty',
                            'last', 'volume', 'vwap',
                            'low', 'high', 'change',
                            'change_pct'] , "Columns in L1 table are not correct."    


def test_sql_L2():
    """
    Tests the SQL output mode for the L2 orderbook.
    """

    # Spin up the KrakenL2 object.
    l2feed = KrakenL2(
        ["DOGE/USD", "BTC/USD"],    # List or str of requested symbols.
        # ["DOGE/USD"],    # List or str of requested symbols.
    log_book_every=1,         # How often to log L2 book info.
    append_book=True,           # Append book mode, saves historical L2 data.
    output_directory="data",  # Output directory for L2 data files.
    trace=False,                # Do not clog output with WS info.
    output_mode="sql"
    )


    # Start a background process with the KrakenL2 object.
    thread = threading.Thread(target=l2feed.launch)        
    thread.daemon=True
    thread.start()

    # Give it enough time to pull some data/store in the SQL database.
    time.sleep(5)

    # Stop the background process.
    l2feed.stop_websocket()

    # Connect to the database and grab a cursor.
    conn = sqlite3.connect("data/kracked_outputs.db")
    cur = conn.cursor()

    res = cur.execute("SELECT name from sqlite_master where type='table'")

    table_names = [v[0] for v in res.fetchall()]

    assert "L2" in table_names, "L2 table not created"

    res = cur.execute("PRAGMA table_info(L2)")
    column_names = [r[1] for r in res.fetchall()]

    assert column_names == ['timestamp',
                            'ask_px_0',
                            'ask_sz_0',
                            'bid_px_0',
                            'bid_sz_0',
                            'ask_px_1',
                            'ask_sz_1',
                            'bid_px_1',
                            'bid_sz_1',
                            'ask_px_2',
                            'ask_sz_2',
                            'bid_px_2',
                            'bid_sz_2',
                            'ask_px_3',
                            'ask_sz_3',
                            'bid_px_3',
                            'bid_sz_3',
                            'ask_px_4',
                            'ask_sz_4',
                            'bid_px_4',
                            'bid_sz_4',
                            'ask_px_5',
                            'ask_sz_5',
                            'bid_px_5',
                            'bid_sz_5',
                            'ask_px_6',
                            'ask_sz_6',
                            'bid_px_6',
                            'bid_sz_6',
                            'ask_px_7',
                            'ask_sz_7',
                            'bid_px_7',
                            'bid_sz_7',
                            'ask_px_8',
                            'ask_sz_8',
                            'bid_px_8',
                            'bid_sz_8',
                            'ask_px_9',
                            'ask_sz_9',
                            'bid_px_9',
                            'bid_sz_9'] , "Column names not correct in L2 table."





