# kracked
Efficiency-first framework for pulling, storing, and aggregating Kraken data
using the v2 API. 

1. All market data is avaiable through WebSocket subscription using this package
by leveraging the Kraken v2 API (see for example [their excellent API](https://docs.kraken.com/api/docs/websocket-v2/add_order)).
2. See the code or the example files for a general overview of the type of files
that will be created for you upon subscription to the data feeds.
3. Order placement (for now) relies on the REST API. See below in the Usage
section.

## Installation
Soon I will push the published code to the PyPI, allowing for you to run 

```pip install kracked```

However in the meanwhile, simply clone the repository, navigate to the folder with `setup.py` and run 

```pip install -e .``` for an editable install.

## Comments
The `kracked` package is still in pre-release and should be considered highly 
experimental. Do not build out any excessively time-intensive routines using 
the package as many things are subject to change (in the near future). The code
is relased AS-IS and the developers are not responsible for any bugs,
misbehavior of the code, or anything of the sort. Any loss of funds resulting from
the utilization of this code is 100% the responsibilty of the user.

## Usage
Users can find a set of example scripts in the examples folder. A couple notes 
about using these files.

1. Ensure that your outputs correspond (qualitatively) to those in the `ex_out/` 
folder if you are using the single-threaded approaches in the `examples/` folder.
2. `kracked` supports multithreaded feeds using the `kracked/manager.py` module. If
you choose to use this approach, ensure that your outputs correspond (again 
qualitatively) to those in `examples/ex_multifeed_out/`. This makes it simple to 
spin up a feed subscribing to multiple Kraken v2 API endpoints using a single
core. See the `multifeed_examples.py` file.

***IMPORTANT*** See the logic in `multifeed_examples.py` for a way to have the connections automatically
reconnect to the Websocket in the event of one feed timing out.


4. Examples of how to load and parse the data are available in the 
`example_data_loading.ipynb` files. These are for the older .csv formats, but shortly I'll update these
with the parquet I/O.

6. For now, order execution simply relies on the CCXT->Kraken REST API. This
will change in the near future, but keep in mind that ANY algorithmic trading
strategies are 100% subject to be rendered incompatible with EITHER changes in 
CCXT, or changes associated with order placement using the Kraken REST API.

## Webapp
Check back soon for more information on the webapp. I'll be integrating features for tracking 
live performance, multiple symbols and the like. For now, consider it highly experimental, but 
feel free to play with the app.py files/settings. It can currently visualize the live L2 book,
price data, spread, and any entries/exits associated wtih algorithmic trading strategies. This
element is subject to potentially being moved to its own package at some point if it can be
sufficiently well generalized to different data sources. 

**WARNING** To those using the webapp, it is not yet setup to work with the parquet IO changes.

### Structure currently of the code base:
```
├── LICENSE
├── README.md
├── examples
│   ├── L1_example.py
│   ├── L2_example.py
│   ├── L3_example.py
│   ├── OHLC_example.py
│   ├── ex_multifeed_out
│   │   ├── ...
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
