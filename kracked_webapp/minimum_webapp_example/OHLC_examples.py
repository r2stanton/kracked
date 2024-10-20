from kracked.feeds import KrakenOHLC
import toml

with open(f"/home/alg/.api.toml", "r") as fil:
    data = toml.load(fil)
api_key = data["kraken_api"]
api_secret = data["kraken_sec"]

ohlcfeed = KrakenOHLC(
    "BTC/USD",
    interval=1,
    trace=False,
    api_key=api_key,
    secret_key=api_secret,
    output_directory="data"
)

ohlcfeed.launch()
