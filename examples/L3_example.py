from kracked.feeds import KrakenL3, KrakenL2
import toml, os


if not os.path.exists("data"):
    os.system("mkdir data")


# Load in your API and SECRET key with the preferred method.
# Here is an example to load from a toml file.
with open(f"/home/alg/.api.toml", "r") as fil:
    data = toml.load(fil)
api_key = data['kraken_api']
api_secret = data['kraken_sec']

if not os.path.exists("data/"):
    os.system("mkdir data")
l3feed = KrakenL3(["DOGE/USD", "BTC/USD", "ETH/USD", "SOL/USD"],
                  api_key=api_key,
                  secret_key=api_secret,
                  trace=False,
                  out_file_name='data/output.csv')

l3feed.launch()
