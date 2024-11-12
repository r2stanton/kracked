from kracked.feeds import KrakenL3
import toml

#api_key = YOUR_API_KEY
#api_secret = YOUR_SECRET_KEY

l3feed = KrakenL3(
    ["DOGE/USD", "BTC/USD"],        # List or str of requested symbols.
    api_key=api_key,                # Your API key for Kraken.
    secret_key=api_secret,          # Your Secrete key for Kraken.
    output_directory='ex_out',          # Output directory for L3 data files.
    trace=False,                    # Do not clog output with WS info.
)
l3feed.launch()
