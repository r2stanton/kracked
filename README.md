# kracked
Efficiency-first framework for pulling, storing, and aggregating Kraken data
using the v2 API. 

1. All market data is avaiable through WebSocket subscription using this package
by leveraging the Kraken v2 API (see for example [their excellent API](https://docs.kraken.com/api/docs/websocket-v2/add_order)).
2. See the code or the example files for a general overview of the type of files
that will be created for you upon subscription to the data feeds.
3. Order placement (for now) relies on the REST API. See below in the Usage
section.


## Comments
The Kracked package is still in pre-release and should be considered highly 
experimental. Do not build out any excessively time-intensive routines using 
the package as many things are subject to change (in the near future). The code
is relased AS-IS and the developers are not responsible for any bugs,
misbehavior of the code, or anything of the sort. Any loss of funds resulting from
the utilization of this code is 100% the responsibilty of the user.

## Usage
Users can find a set of example scripts in the examples folder. A couple notes 
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
4. For now, order execution simply relies on the CCXT->Kraken REST API. This
will change in the near future, but keep in mind that ANY algorithmic trading
strategies are 100% subject to be rendered incompatible with EITHER changes in 
CCXT, or changes associated with order placement using the Kraken REST API.

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
│   ├── manager.py
│   ├── feeds.py
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
1. Efficiency improvements (periodic transition from csv->parquet.
2. Compliance with Databento schemas where applicable.
