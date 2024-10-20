from kracked.core import BaseKrakenWS
from zlib import crc32 as CRC32
import numpy as np
import toml, json, os
import datetime

class KrakenL1(BaseKrakenWS):
    """
    Class extending BaseKrakenWS geared towards L3 feeds from the Kraken v2 API.
    """

    def __init__(
        self,
        symbols,
        api_key=None,
        secret_key=None,
        trace=False,
        output_directory=".",
    ):

        if type(symbols) == str:
            symbols = [symbols]

        self.symbols = symbols
        self.auth = False
        self.trace = trace
        self.output_directory = output_directory

    def _on_error(self, ws, error):
        print("Error in L1 Feed")
        print(error)


    def _on_message(self, ws, message):
        response = json.loads(message)
        reponse_keys = list(response.keys())

        if "channel" in reponse_keys:

            # Main case for handling the OHLC data from Kraken
            if response["channel"] == "ticker":

                # ┆  96 {'channel': 'ohlc', 'type': 'update', 'timestamp': '2024-10-11T01:20:09.952961122Z',
                # 'data': [{'symbol      ': 'DOGE/USD', 'open': 0.1058763, 'high': 0.1058763, 'low': 0.1058763,
                # 'close': 0.1058763, 'trades': 1      , 'volume': 266.663, 'vwap': 0.1058763,
                #'interval_begin': '2024-10-11T01:20:00.000000000Z', 'interval'      : 1,
                # 'timestamp': '2024-10-11T01:21:00.000000Z'}]}

                if response["type"] in ["update", "snapshot"]:
                    full_data = response["data"]
                    # assert len(full_data) > 1, "Data shorter than expected"

                    info_lines = []
                    for data in full_data:
                        symbol = data["symbol"]
                        bid = data["bid"]
                        bid_qty = data["bid_qty"]
                        ask = data["ask"]
                        ask_qty = data["ask_qty"]
                        last = data["last"]
                        volume = data["volume"]
                        vwap = data["vwap"]
                        low = data["low"]
                        high = data["high"]
                        change = data["change"]
                        change_pct = data["change_pct"]

                        info = [
                            symbol,
                            str(bid),
                            str(bid_qty),
                            str(ask),
                            str(ask_qty),
                            str(last),
                            str(volume),
                            str(vwap),
                            str(low),
                            str(high),
                            str(change),
                            str(change_pct),
                        ]

                        info_lines.append(info)


                    for info in info_lines:
                        if not os.path.exists(f"{self.output_directory}/L1.csv"):
                            with open(f"{self.output_directory}/L1.csv", "w") as fil:
                                fil.write(
                                    "symbol,bid,bid_qty,ask,ask_qty,last,volume,vwap,low,high,change,change_pct\n"
                                )
                                fil.write(",".join(info) + "\n")
                        else:
                            with open(f"{self.output_directory}/L1.csv", "a") as fil:
                                fil.write(",".join(info) + "\n")

            elif response["channel"] in ["heartbeat", "status", "subscribe"]:
                pass

    def _on_open(self, ws):
        """
        Open message for Kraken L1 connection.
        """

        print("Kraken v2 Connection Opened.")

        subscription = {
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": self.symbols,
            },
        }
        ws.send(json.dumps(subscription))


class KrakenL2(BaseKrakenWS):
    """
    Class extending BaseKrakenWS geared towards L3 feeds from the Kraken v2 API.
    """

    def __init__(
        self,
        symbols,
        api_key=None,
        secret_key=None,
        trace=False,
        output_directory=".",
        depth=10,
        log_book_every=100,
        log_bbo_every=200,
    ):

        assert depth in [
            10,
            25,
            100,
            500,
            1000,
        ], "Depths allowed: 10, 25, 100, 500, 1000"

        if type(symbols) == str:
            symbols = [symbols]

        self.depth = depth
        self.symbols = symbols
        self.auth = False
        self.trace = trace
        self.log_book_every = log_book_every
        self.log_bbo_every = log_bbo_every
        self.ask_prices = [0.0] * self.depth
        self.bid_prices = [0.0] * self.depth
        self.ask_volumes = np.zeros(self.depth)
        self.bid_volumes = np.zeros(self.depth)
        self.count = 0
        self.output_directory = output_directory

    def _on_message(self, ws, message):
        response = json.loads(message)
        self.count += 1

        # Pass all method messages.
        if "method" in response.keys():
            pass

        if "channel" in response.keys():

            # Skip heartbeats and status messges.
            if response["channel"] == "heartbeat" or response["channel"] == "status":
                pass

            # SNAPSHOTS RESET THE ORDERBOOK FROM OUR MANUAL UPDATE PROCESS.
            elif response["type"] == "snapshot":
                # Pull data
                data = response["data"]
                if len(data) != 1:
                    raise ValueError("Data longer than expected")
                data = data[0]
                checksum = data["checksum"]

                # Bids -> Asserts snapshot fills the orderbook (w.r.t self.depth)
                bids = data["bids"]
                assert len(bids) == self.depth, "Snapshot should be full book refresh."

                # Asks -> Asserts snapshot fills the orderbook (w.r.t self.depth)
                asks = data["asks"]
                assert len(asks) == self.depth, "Snapshot should be full book refresh."

                self.bids = {b["price"]: b["qty"] for b in bids}
                self.asks = {a["price"]: a["qty"] for a in asks}

                self.bid_prices = [b["price"] for b in bids]
                self.ask_prices = [a["price"] for a in asks]

                self._book_checksum(ws, checksum)

                print(self.bids)
                print(self.asks)
                print(self.bid_prices)
                print(self.ask_prices)
                print("THIS MANY BIDS:")
                print(len(list(self.bids.keys())))
                # print(len(list(self.asks.keys())))

            else:
                self.count += 1

                data = response["data"]
                if ("bids" in response["data"][0].keys()) & (
                    "asks" in response["data"][0].keys()
                ):

                    bids = response["data"][0]["bids"]
                    asks = response["data"][0]["asks"]

                    bqty = [b["qty"] for b in bids]
                    aqty = [a["qty"] for a in asks]
                    new_bids = [
                        b["price"] for b in bids if b["price"] not in self.bid_prices
                    ]
                    new_asks = [
                        a["price"] for a in asks if a["price"] not in self.ask_prices
                    ]

                    # Check if we remove any price levels from the book
                    if 0 in bqty:
                        # Check if we have actual bids to update.
                        if len(new_bids) == 0:
                            raise ValueError("REMOVING PRICE LEVEL WITHOUT REPLACEMENT")

                    if 0 in aqty:
                        # Check if we have actual bids to update.
                        if len(new_asks) == 0:
                            raise ValueError("REMOVING PRICE LEVEL WITHOUT REPLACEMENT")

                    if len(bids) > 0:
                        for bid in bids:
                            curr_price = bid["price"]

                            # REMOVE BAD PRICE LEVEL
                            if bid["qty"] == 0:
                                # Remove the price level from the dictionary
                                del self.bids[curr_price]
                                # Zero this bad price level in the self.prices array.
                                self.bid_prices.remove(curr_price)

                            # ADD GOOD PRICE LEVEL
                            else:
                                self.bids[curr_price] = bid["qty"]
                                if curr_price not in self.bid_prices:
                                    self.bid_prices.append(curr_price)

                        self.bid_prices.sort(reverse=True)

                        # HANDLE BOOKKEEPING OF THE DEPTH FOR MBP DATA
                        num_bid_levels = len(self.bid_prices)
                        if num_bid_levels == self.depth:
                            pass
                        elif num_bid_levels > self.depth:
                            # Remove all other price levels
                            bad_prices = self.bid_prices[self.depth :]
                            for bp in bad_prices:
                                self.bid_prices.remove(bp)
                                del self.bids[bp]
                        else:
                            ws.close()
                            raise ValueError(f"MBP Depth is lower than {self.depth}")

                    if len(asks) > 0:
                        for ask in asks:
                            curr_price = ask["price"]

                            # REMOVE BAD PRICE LEVEL
                            if ask["qty"] == 0:
                                # Remove the price level from the dictionary
                                del self.asks[curr_price]
                                # Zero this bad price level in the self.prices array.
                                self.ask_prices.remove(curr_price)

                            # ADD GOOD PRICE LEVEL
                            else:
                                self.asks[curr_price] = ask["qty"]
                                if curr_price not in self.ask_prices:
                                    self.ask_prices.append(curr_price)

                        self.ask_prices.sort(reverse=False)

                        # HANDLE BOOKKEEPING OF THE DEPTH FOR MBP DATA
                        num_ask_levels = len(self.ask_prices)
                        if num_ask_levels == self.depth:
                            pass
                        elif num_ask_levels > self.depth:
                            # Remove all other price levels
                            bad_prices = self.ask_prices[self.depth :]
                            for bp in bad_prices:
                                self.ask_prices.remove(bp)
                                del self.asks[bp]
                        else:
                            ws.close()
                            raise ValueError(f"MBP Depth is lower than {self.depth}")

                if self.count % self.log_book_every:
                    output = True
                    full_L2_orderbook = {"b": self.bids, "a": self.asks}

                    with open(f"{self.output_directory}/L2_orderbook.json", "w") as fil:
                        json.dump(full_L2_orderbook, fil)
                else:
                    output = False

                if self.count % self.log_bbo_every == 0:
                    print("here")
                    if not os.path.exists(f"{self.output_directory}/L1_BBO.csv"):
                        with open(f"{self.output_directory}/L1_BBO.csv", "w") as fil:
                            fil.write("timestamp,bbo,bao\n")
                    else:
                        with open(f"{self.output_directory}/L1_BBO.csv", "a") as fil:
                            now = datetime.datetime.now()
                            BBO = np.max(self.bid_prices)
                            BAO = np.min(self.ask_prices)
                            fil.write(f"{now},{BBO},{BAO}\n")

                if output:
                    print("Best Bid \t\t Best Ask \t\t Spread")
                    BBO = np.max(self.bid_prices)
                    BAO = np.min(self.ask_prices)
                    print(f"{BBO}\t\t{BAO}\t\t{BAO-BBO}")

    def _book_checksum(self, ws, checksum):

        bid_keys = list(self.bids.keys())
        ask_keys = list(self.asks.keys())
        bid_vals = list(self.bids.values())
        ask_vals = list(self.asks.values())

        asksum = ""
        bidsum = ""
        for i in range(10):

            # String bid price, String bid ask
            sbp = str(bid_keys[i]).replace(".", "").lstrip("0")
            sap = str(ask_keys[i]).replace(".", "").lstrip("0")

            # String bid qty, String bid ask
            sbq = f"{self.bids[bid_keys[i]]:.8f}".replace(".", "").lstrip("0")
            saq = f"{self.asks[ask_keys[i]]:.8f}".replace(".", "").lstrip("0")

            sb = sbp + sbq
            sa = sap + saq

            asksum += sa
            bidsum += sb

        csum_string = asksum + bidsum
        kracked_checksum = CRC32(csum_string.encode("utf-8"))

        if checksum != kracked_checksum:
            # FIXME ADD ERROR HANDLING HERE.
            raise ValueError("CHECKSUM MISMATCH! BOOK IS INCONSISTENT!")

    def _on_open(self, ws):
        """
        Open message for Kraken L3 connection.
        """

        print("Kraken v2 Connection Opened.")
        # ws_token = self.get_ws_token(self.api_key, self.api_secret)

        subscription = {
            "method": "subscribe",
            "params": {"channel": "book", "symbol": self.symbols,
            # "token": ws_token
            },
        }

        ws.send(json.dumps(subscription))


class KrakenL3(BaseKrakenWS):
    """
    Class extending BaseKrakenWS geared towards L3 feeds from the Kraken v2 API.
    """

    def __init__(
        self,
        symbols,
        api_key=None,
        secret_key=None,
        trace=False,
        out_file_name="L3_ticks.csv",
        log_ticks_every=100,
        log_for_webapp=False,
        output_directory=".",
    ):

        self.tick_count = 0
        if type(symbols) == str:
            symbols = [symbols]

        self.symbols = symbols
        self.auth = True
        self.trace = trace
        self.api_key = api_key
        self.api_secret = secret_key
        self.log_ticks_every = log_ticks_every
        self.out_file_name = out_file_name
        self.ticks = []
        self.output_directory = output_directory
        self.log_for_webapp = log_for_webapp

    def _on_message(self, ws, message):
        response = json.loads(message)
        if len(self.ticks) > self.log_ticks_every:
            if not os.path.exists(f"{self.output_directory}/{self.out_file_name}"):
                with open(f"{self.output_directory}/{self.out_file_name}", "w") as fil:
                    fil.write("side,timestamp,price,size,event,order_id\n")
            else:
                with open(f"{self.output_directory}/{self.out_file_name}", "a") as fil:
                    for tick in self.ticks:
                        tick = [str(t) for t in tick]
                        fil.write(",".join(tick) + "\n")
            self.ticks = []
            self.tick_count = 0

        # print(response)
        if "data" in response.keys() and response["type"] != "snapshot":
            assert len(response["data"]) == 1, "Haven't seen this response before"
            if (
                "bids" in response["data"][0].keys()
                and "asks" in response["data"][0].keys()
            ):
                bids = response["data"][0]["bids"]
                asks = response["data"][0]["asks"]
                if len(bids) > 0:
                    for bid in bids:
                        info = [
                            "b",  # Side
                            bid["timestamp"],  # Time
                            bid["limit_price"],  # Price
                            bid["order_qty"],  # Size
                            bid["event"],  # Event
                            bid["order_id"],  # OID
                        ]
                        self.ticks.append(info)
                        self.tick_count += 1
                if len(asks) > 0:
                    for ask in asks:
                        info = [
                            "a",  # Side
                            ask["timestamp"],  # Time
                            ask["limit_price"],  # Price
                            ask["order_qty"],  # Size
                            ask["event"],  # Event
                            ask["order_id"],  # OID
                        ]
                        self.ticks.append(info)
                        self.tick_count += 1
                if len(asks) == 0 and len(bids) == 0:
                    pass

        elif "data" in response.keys() and response["type"] == "snapshot":
            # Decide what to do with initial snapshot later
            print("SKIPPING SNAPSHOT")
            pass

    def _on_open(self, ws):
        """
        Open message for Kraken L3 connection.
        """

        print("Kraken v2 Connection Opened.")
        ws_token = self.get_ws_token(self.api_key, self.api_secret)

        subscription = {
            "method": "subscribe",
            "params": {"channel": "level3", "symbol": self.symbols, "token": ws_token},
        }

        ws.send(json.dumps(subscription))


class KrakenOHLC(BaseKrakenWS):
    """
    Class extending BaseKrakenWS geared towards L3 feeds from the Kraken v2 API.
    """

    def __init__(
        self,
        symbols,
        api_key=None,
        secret_key=None,
        trace=False,
        interval=5,
        output_directory=".",
    ):

        all_int = [1, 5, 15, 30, 60, 240, 1440, 10080, 21600]

        assert interval in all_int, f"Choose interval from {all_int}"

        self.tick_count = 0
        if type(symbols) == str:
            symbols = [symbols]

        self.symbols = symbols
        self.auth = False
        self.trace = trace
        self.ticks = []
        self.interval = interval
        self.output_directory = output_directory


    def _on_message(self, ws, message):
        print(message)
        response = json.loads(message)

        reponse_keys = list(response.keys())

        if "channel" in reponse_keys:

            # Main case for handling the OHLC data from Kraken
            if response["channel"] == "ohlc":

                # ┆  96 {'channel': 'ohlc', 'type': 'update', 'timestamp': '2024-10-11T01:20:09.952961122Z',
                # 'data': [{'symbol      ': 'DOGE/USD', 'open': 0.1058763, 'high': 0.1058763, 'low': 0.1058763,
                # 'close': 0.1058763, 'trades': 1      , 'volume': 266.663, 'vwap': 0.1058763,
                #'interval_begin': '2024-10-11T01:20:00.000000000Z', 'interval'      : 1,
                # 'timestamp': '2024-10-11T01:21:00.000000Z'}]}

                if response["type"] == "update":
                    data = response["data"]
                    assert len(data) == 1, "Data longer than expected"
                    data = data[0]

                    symbol = data["symbol"]
                    open_p = data["open"]
                    high = data["high"]
                    low = data["low"]
                    close = data["close"]
                    trades = data["trades"]
                    volume = data["volume"]
                    vwap = data["vwap"]
                    tend = data["timestamp"]
                    tstart = data["interval_begin"]
                    ttrue = response["timestamp"]

                    info = [
                        tend,
                        symbol,
                        str(open_p),
                        str(high),
                        str(low),
                        str(close),
                        str(volume),
                        str(vwap),
                        str(trades),
                        tstart,
                        ttrue,
                    ]

                    if not os.path.exists(f"{self.output_directory}/ohlc.csv"):
                        with open(f"{self.output_directory}/ohlc.csv", "a") as fil:
                            fil.write(
                                "tend,open,high,low,close,volume,vwap,trades,tstart,ttrue\n"
                            )
                            fil.write(",".join(info) + "\n")

                    with open(f"{self.output_directory}/ohlc.csv", "a") as fil:
                        fil.write(",".join(info) + "\n")

                elif response["type"] == "snapshot":
                    full_data = response["data"]
                    assert len(full_data) > 1, "Data shorter than expected"

                    info_lines = []
                    for data in full_data:
                        symbol = data["symbol"]
                        open_p = data["open"]
                        high = data["high"]
                        low = data["low"]
                        close = data["close"]
                        trades = data["trades"]
                        volume = data["volume"]
                        vwap = data["vwap"]
                        tend = data["timestamp"]
                        tstart = data["interval_begin"]
                        ttrue = response["timestamp"]

                        info = [
                            tend,
                            symbol,
                            str(open_p),
                            str(high),
                            str(low),
                            str(close),
                            str(volume),
                            str(vwap),
                            str(trades),
                            tstart,
                            ttrue,
                        ]

                        info_lines.append(info)


                    for info in info_lines:
                        if not os.path.exists(f"{self.output_directory}/ohlc.csv"):
                            with open(f"{self.output_directory}/ohlc.csv", "w") as fil:
                                fil.write(
                                    "tend,symbol,open,high,low,close,volume,vwap,trades,tstart,ttrue\n"
                                )
                        else:
                            with open(f"{self.output_directory}/ohlc.csv", "a") as fil:
                                fil.write(",".join(info) + "\n")

            elif response["channel"] in ["heartbeat", "status", "subscribe"]:
                pass

    def _on_open(self, ws):
        """
        Open message for Kraken L3 connection.
        """

        print("Kraken v2 Connection Opened.")

        subscription = {
            "method": "subscribe",
            "params": {
                "channel": "ohlc",
                "symbol": self.symbols,
                "interval": self.interval,
            },
        }

        ws.send(json.dumps(subscription))


class KrakenTrades(BaseKrakenWS):
    """
    Websocket for the Trade endpoint from the Kraken v2 API.

    This channel generates a trade event whenever there is an order matched in the book.
    """

    def __init__(self, symbols, api_key=None, secret_key=None, trace=False,
    log_trades_every=100, output_directory="."):
        """

        Constructor for the KrakenTrades endpoint.

        Parameters:
        -----------
            symbols List[str] or str:
            api_key: str
            secret_key: str
            trace: bool
            log_trades_every: int
            output_directory: str
        """

        if type(symbols) == str:
            symbols = [symbols]

        self.symbols = symbols
        self.auth = False
        self.trace = trace
        self.log_trades_every = log_trades_every
        self.output_directory = output_directory
        self.all_trades = []

    def _on_message(self, ws, message):

        response = json.loads(message)

        reponse_keys = list(response.keys())

        if "channel" in reponse_keys:
            print("New ping from Kraken")
            if response["channel"] == "trade":
                if response["type"] in ["update", "snapshot"]:
                    filled_trades = response['data']
                    for trade in filled_trades:
                        ts_event = trade['timestamp']
                        symbol = trade['symbol']
                        price = str(trade['price'])
                        qty = str(trade['qty'])
                        side = trade['side']
                        ord_type = trade['ord_type']
                        trade_id = str(trade['trade_id'])
                        self.all_trades.append([ts_event, symbol, price, qty, side, ord_type, trade_id])

                elif response["type"] == "snapshot":
                    pass
            elif response["channel"] in ["heartbeat", "status", "subscribe"]:
                pass

        if len(self.all_trades) >= self.log_trades_every:

            if not os.path.exists(f"{self.output_directory}/trades.csv"):
                print(f"Writing file {self.output_directory}/trades.csv")
                with open(f"{self.output_directory}/trades.csv", "w") as fil:
                    fil.write("ts_event,symbol,price,qty,side,ord_type,trade_id\n")

            with open(f"{self.output_directory}/trades.csv", "a") as fil:
                for trade in self.all_trades:
                    fil.write(",".join(trade) + "\n")

            self.all_trades = []


    def _on_open(self, ws):

        print("Kraken v2 Connection Opened.")

        subscription = {
        "method": "subscribe",
        "params": {
            "channel": "trade",
            "symbol": self.symbols,
            "snapshot": True
        }
        }

        ws.send(json.dumps(subscription))


class KrakenInstruments(BaseKrakenWS):
    def __init__(self, trace=False, output_directory="."):

        self.auth = False
        self.trace = trace
        self.output_directory = output_directory

    def _on_message(self, ws, message):
        response = json.loads(message)
        if response["channel"] == "status":
            pass
        else:

            if response["type"] == "snapshot":
                assets = response["data"]["assets"]
                pairs = response["data"]["pairs"]
                print(len(assets), len(pairs))
                unique_quotes = []

                header_pairs = pairs[0].keys()
                header_assets = assets[0].keys()

                keys = [
                    "symbol", "base", "quote", "status", "qty_precision", "qty_increment", "price_precision",
                    "cost_precision", "marginable", "has_index", "cost_min", "margin_initial", "position_limit_long",
                    "position_limit_short", "tick_size", "price_increment", "qty_min"
                ]
                
                pair_info = []
                for pair in pairs:
                    for key in keys:
                        if key not in pair.keys():
                            pair[key] = "None"

                    pair_info.append([str(pair[key]) for key in keys])

                asset_info = [[str(x) for x in a.values()] for a in assets]

                with open(f'{self.output_directory}/kraken_pairs.csv', 'w') as fil:
                    fil.write(','.join(keys) + '\n')
                    for line in pair_info:
                        fil.write(','.join(line) + '\n')

                with open(f'{self.output_directory}/kraken_assets.csv', 'w') as fil:
                    fil.write(','.join(header_assets) + '\n')
                    for line in asset_info:
                        fil.write(','.join(line) + '\n')


                ws.close()
                exit(1)

                    # if pair["symbol"] == "DOGE/USD":
                    #     print(pair)

    def _on_open(self, ws):

        print("Kraken v2 Connection Opened.")
        # ws_token = self.get_ws_token(self.api_key, self.api_secret)

        subscription = {
            "method": "subscribe",
            "params": { "channel": "instrument" },
            "req_id": 79 
            }

        ws.send(json.dumps(subscription))


class KrakenPlaceholder(BaseKrakenWS):
    def __init__(self, symbols, api_key=None, secret_key=None, trace=False):
        ...

    def _on_message(self, ws, message):
        ...

    def _on_open(self, ws):
        ...
    


if __name__ == "__main__":
    with open(f"/home/alg/.api.toml", "r") as fil:
        data = toml.load(fil)
    api_key = data["kraken_api"]
    api_secret = data["kraken_sec"]


    instruments = KrakenInstruments(trace=False, output_directory=".")
    instruments.launch()
    # os.system("rm data/trades.csv")
    # tradefeed = KrakenTrades("BTC/USD", trace=False, api_key=api_key, secret_key=api_secret, write_every=1,
    # output_directory=".")
    # tradefeed.launch()


    # L1feed = KrakenL1("BTC/USD", api_key=api_key, secret_key=api_secret, trace=False)
    # L1feed.launch()
