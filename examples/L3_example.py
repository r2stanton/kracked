from kracked.feeds import KrakenL3
import toml

# Load/type directly as arguments to the KrakenL3 constructor your API info.
with open(f"/home/alg/.api.toml", "r") as fil:
    data = toml.load(fil)
api_key = data['kraken_api']
api_secret = data['kraken_sec']

l3feed = KrakenL3(["DOGE/USD", "BTC/USD", "ETH/USD", "SOL/USD"],
                  api_key=api_key, secret_key=api_secret,
                  trace=False, out_file_name='./output.csv')
l3feed.launch()
