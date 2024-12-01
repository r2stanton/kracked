from kracked.feeds import KrakenTrades

tradefeed = KrakenTrades(
    ["BTC/USD", "DOGE/USD"],    # List or str of requested symbols.
    output_directory="ex_out",  # Output directory for L1 data files.
    # trace=False,                # Do not clog output with WS info.
    # log_trades_every=1,         # How often 
    log_trades_every=4,
    parquet_flag=True
)

tradefeed.launch()
