from dolfin.labels.features import sma
from dolfin.labels.signals import crossover
from kracked.actions import add_order
import ccxt, time, toml
import pandas as pd
import numpy as np

with open("/home/alg/.api.toml", "r") as fil:
    data = toml.load(fil)
api_key = data["kraken_api"]
api_secret = data["kraken_sec"]



file_line = 1
num_buys = 0
num_sells = 0
while num_buys < 100:
    print("NEW CHECK")
    print("NUM BUYS:", num_buys)
    print("NUM SELLS:", num_sells)


    kraken_ccxt = ccxt.kraken({
        'apiKey': api_key,
        'secret': api_secret,
    })


    df = pd.read_csv("ex_out/OHLC.csv")
    df = df[df['symbol'] == 'DOGE/USD']
    df = df.groupby('timestamp').last().reset_index()

    if len(df) > 10:

        sma(df, 'close', 5, min_periods=1)
        sma(df, 'close', 10, min_periods=1)

        crossover(df, 'SMA_5_close', 'SMA_10_close')

        if df.iloc[-1]['crossover']:
            print("FOUND CROSSOVER")

            if df.iloc[-1]['c1_gt_c2']:
                print("SMA5 crossed above SMA10, buying.")
                add_order(kraken_ccxt,
                          "market",
                          "DOGE/USD",
                          "buy",
                          30.5, time_in_force='gtc', )
                num_buys += 1
            else:
                if num_buys > 0:
                    print("SMA10 crossed above SMA5, selling.")
                    add_order(kraken_ccxt,
                              "market",
                              "DOGE/USD",
                              "sell",
                              30.5, time_in_force='gtc', )
                    num_sells +=1
                else:
                    print("SMA10 crossed above SMA5, but no assets to sell")

    time.sleep(300)

