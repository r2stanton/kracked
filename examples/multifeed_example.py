from kracked.manager import KrakenFeedManager
import toml, time

# Load/type directly as arguments to the KrakenL3 constructor your API info.
with open(f"/home/alg/.api.toml", "r") as fil:
    data = toml.load(fil)
api_key = data['kraken_api']
api_secret = data['kraken_sec']


feeds = KrakenFeedManager(["DOGE/USD", "BTC/USD", "SOL/USD"],
                          api_key, 
                          api_secret,
                          L1=True,
                          L2=True,
                          L3=True,
                          trades=True,
                          ohlc=True,
                          output_directory="ex_multifeed_out",

                          L2_params={'log_book_every':50,},
                          L3_params={'log_ticks_every':100},
                          ohlc_params={'interval':5},
                          trades_params={'log_trades_every':50}
                          )



feeds.start_all()

ct = 0
try:
    while True:
        ct += 1
        time.sleep(1)
        if ct % 50 == 0:
            # Loop through the feeds and ping Kraken
            ...


except KeyboardInterrupt:
    print("Shutting down feeds...")
    feeds.stop_all()

