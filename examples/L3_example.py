from kracked.feeds import KrakenL3

api_key = ''
api_secret = ''

l3feed = KrakenL3(
    ["DOGE/USD", "BTC/USD"],    # List or str of requested symbols.
    api_key=api_key,            # Your API key for Kraken.
    secret_key=api_secret,      # Your secret key for Kraken.
    output_directory="ex_out",  # Output directory for L3 data files.
    output_mode="parquet",      # "csv", "parquet", or "sql"
    trace=False,                # Do not clog output with WS info.
)

l3feed.launch()
