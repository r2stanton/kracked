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

## Usage
Users can find a set of example scripts in the examples folder. A couple notes 
about using these files.
