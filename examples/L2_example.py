from kracked.feeds import KrakenL2

l2feed = KrakenL2(
    ["BTC/USD"],    # List or str of requested symbols.
    # ["DOGE/USD"],    # List or str of requested symbols.
    log_book_every=1,         # How often to log L2 book info.
    append_book=True,           # Append book mode, saves historical L2 data.
    output_directory="ex_out",  # Output directory for L2 data files.
    trace=False,                # Do not clog output with WS info.
)
    
l2feed.launch()                 # Launch the L2 data feed.
