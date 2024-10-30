from kracked.feeds import KrakenOHLC

ohlcfeed = KrakenOHLC(
    ["BTC/USD", "DOGE/USD"],# List or str of requested symbols.
    interval=1,             # Interval in minutes.
    output_directory="ex_out",# Output directory for L1 data files.
    trace=False,            # Do not clog output with WS info.
)

ohlcfeed.launch()
