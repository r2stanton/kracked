from kracked.feeds import KrakenL3
import toml

# Load/type directly as arguments to the KrakenL3 constructor your API info.
with open(f"/home/alg/.api.toml", "r") as fil:
    data = toml.load(fil)
api_key = data['kraken_api']
api_secret = data['kraken_sec']

l3feed = KrakenL3(
    ["DOGE/USD", "BTC/USD"],        # List or str of requested symbols.
    api_key=api_key,                # Your API key for Kraken.
    secret_key=api_secret,          # Your Secrete key for Kraken.
    output_directory='ex_out',          # Output directory for L3 data files.
    trace=False,                    # Do not clog output with WS info.
)
l3feed.launch()
