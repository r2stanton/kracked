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


l2feed = KrakenL2("BTC/USD",
                  api_key=api_key,
                  secret_key=api_secret,
                  trace=False,
                  depth=10,
                  output_dir="data",
                  log_every=100,
                  log_bbo_every=10,
                  )

l2feed.launch()
