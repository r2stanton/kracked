from kracked.feeds import KrakenOHLC

ohlcfeed = KrakenOHLC(
    # ["BTC/USD", "DOGE/USD"],# List or str of requested symbols.
    ["DOGE/USD"],# List or str of requested symbols.
    interval=5,             # Interval in minutes.
    output_directory="ex_out",# Output directory for L1 data files.
    trace=False,            # Do not clog output with WS info.
    ccxt_snapshot=True,     # Use the ccxt snapshot (you get more data)
)

# BEWARE if you use ccxt_snaphot, you get 720 historical candles, HOWEVER
# they will not contain information about VWAP, TRADES, etc.

# If you choose not to use ccxt for the historical data from Kraken, you will
# only get 5 historical candlesticks.

ohlcfeed.launch()
