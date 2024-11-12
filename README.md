# kracked
Efficiency-first framework for pulling, storing, and aggregating Kraken data using the v2 API.

## Comments
The Kracked package is still in pre-release and should be considere highly 
experimental. Do not build out any excessively time-intensive routines using 
the package as many things are subject to change. The code is relased AS-IS 
and the developers are not responsible for any bugs, misbehavior of the code, 
or anything of the sort. 

## Usage
Users can find a set of example scripts in the exmaples folder. A couple notes 
about using these files.

1. Ensure that your outputs correspond (qualitatively) to those in the ex_out/ 
if you are using the single-threaded approaches in the examples/ folder.
2. kracked supports multithreaded feeds using the kracked/manager.py module. If
you choose to use this approach, ensure that your outputs correspond (again 
qualitatively) to those in examples/ex_multifeed_out/. This makes it simple to 
spin up a feed subscribing to multiple Kraken v2 API endpoints using a single
core. See the multifeed_examples.py file.
3. Examples of how to load and parse the data are available in the 
example_data_loading.ipynb files.

#### Structure currently of the code base:
```
├── LICENSE
├── README.md
├── examples
│   ├── L1_example.py
│   ├── L2_example.py
│   ├── L3_example.py
│   ├── OHLC_example.py
│   ├── ex_multifeed_out
│   │   ├── L2_BTC_USD_orderbook.csv
│   │   ├── L2_DOGE_USD_orderbook.csv
│   │   ├── L2_ETH_USD_orderbook.csv
│   │   ├── L2_SOL_USD_orderbook.csv
│   │   ├── L2_live_orderbooks.json
│   │   ├── L3_ticks.csv
│   │   ├── OHLC.csv
│   ├── ex_out
│   │   ├── L1.csv
│   │   ├── L2_BTC_USD_orderbook.csv
│   │   ├── L2_live_orderbooks.json
│   │   ├── L3_ticks.csv
│   │   ├── OHLC.csv
│   │   └── trades.csv
│   ├── example_data_loading.ipynb
│   ├── load_multifeed.ipynb
│   ├── multifeed_example.py
│   ├── order_examples.py
├── kracked
│   ├── actions.py
│   ├── core.py
│   ├── data
├── kracked_webapp
│   ├── app.py
│   ├── data
│   ├── index.html
│   ├── script.js
│   └── style.css
└── tests
    ├── README.md
    ├── data
    │   └── L2_DOGE_USD_orderbook.csv
    └── test_l2.py

```



To-Do
-----

All Market Feeds Working:
L1, L2, L3
Bars (OHLCV)
Trade
Instruments*

Mult-threaded Feed Manager
