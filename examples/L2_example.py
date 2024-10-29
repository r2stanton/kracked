from kracked.feeds import KrakenL2
l2feed = KrakenL2("BTC/USD", trace=False, depth=10, output_directory=".",
                  log_book_every=100, log_bbo_every=10)
l2feed.launch()
