from kracked.feeds import KrakenTrades

tradefeed = KrakenTrades(
    ["BTC/USD", "DOGE/USD"],    # List or str of requested symbols.
    output_directory="ex_out",  # Output directory for trade data files.
    output_mode="parquet",      # "csv", "parquet", or "sql"
    log_trades_every=50,        # Emit a batch after accumulating this many trades.
    trace=False,                # Do not clog output with WS info.
)

tradefeed.launch()
