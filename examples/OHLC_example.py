from kracked.feeds import KrakenOHLC

ohlcfeed = KrakenOHLC(
    ["DOGE/USD"],           # List or str of requested symbols.
    interval=5,             # Interval in minutes.
    output_directory="ex_out",
    output_mode="csv",      # "csv" or "sql"
    trace=False,            # Do not clog output with WS info.
    ccxt_snapshot=True,     # Use the CCXT REST snapshot for deeper history.
)

# BEWARE if you use ccxt_snapshot, you get 720 historical candles, HOWEVER
# they will not contain information about VWAP, TRADES, etc.

# If you choose not to use ccxt for the historical data from Kraken, you will
# only get 5 historical candlesticks.

ohlcfeed.launch()
