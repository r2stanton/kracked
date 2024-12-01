from kracked.manager import KrakenFeedManager
import threading
import toml, time

# Load/type directly as arguments to the KrakenL3 constructor your API info.
with open(f"/home/alg/.api.toml", "r") as fil:
    data = toml.load(fil)
api_key = data['kraken_api']
api_secret = data['kraken_sec']


all_feeds = KrakenFeedManager(["DOGE/USD", "BTC/USD", "SOL/USD", "ETH/USD"],
                              api_key,
                              api_secret,
                              L1=False,
                              L2=True,
                              L3=True,
                              trades=True,
                              ohlc=True,
                              output_directory="ex_multifeed_out",

                              L2_params={'log_book_every': 1},
                              L3_params={'log_ticks_every': 1000},
                              ohlc_params={'interval': 5,
                                           'ccxt_snapshot': True},
                              trades_params={'log_trades_every': 50}
                              )

# BEWARE if you use ccxt_snaphot, you get 720 historical candles, HOWEVER
# they will not contain information about VWAP, TRADES, etc.

# If you choose not to use ccxt for the historical data from Kraken, you will
# only get 5 historical candlesticks.

all_feeds.start_all()

ct = 0
try:
    while True:
        ct += 1
        time.sleep(1)

        for name, thread in zip(all_feeds.threads.keys(),
                                all_feeds.threads.values()):

            if not thread.is_alive():
                print(f"{name} Thread died, trying to restart")
                curr_feed = all_feeds.feeds[name]
                restarted_thread = threading.Thread(target=curr_feed.launch)
                restarted_thread.daemon = True
                restarted_thread.start()
                all_feeds.threads[name] = restarted_thread
                del thread

except KeyboardInterrupt:
    print("Shutting down feeds...")
    all_feeds.stop_all()

