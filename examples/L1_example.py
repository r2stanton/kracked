from kracked.feeds import KrakenL1

l1feed = KrakenL1(
    ["DOGE/USD", "BTC/USD"],    # List or str of requested symbols.
    output_directory="ex_out",  # Output directory for L1 data files.
    trace=False,                # Do not clog output with WS info. 
)

l1feed.launch()
