from kracked.feeds import KrakenL2

l2feed = KrakenL2(
    ["DOGE/USD", "BTC/USD"],    # List or str of requested symbols.
    log_book_every=1,           # Emit a book snapshot every N updates.
    append_book=True,           # Append book mode, saves historical L2 data.
    output_directory="ex_out",  # Output directory for L2 data files.
    output_mode="parquet",      # "csv", "parquet", or "sql"
    trace=False,                # Do not clog output with WS info.
)

l2feed.launch()                 # Launch the L2 data feed.
