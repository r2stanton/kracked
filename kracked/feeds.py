from kracked.core import BaseKrakenWS
from zlib import crc32 as CRC32
import numpy as np
import toml, json, os
import datetime


class KrakenL1(BaseKrakenWS):
    """
    Class extending BaseKrakenWS geared towards L1 feeds from the Kraken v2 API.
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
        recv_ts = datetime.datetime.now()
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
                        timestamp = str(recv_ts)
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
                            timestamp,
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
                                    "timestamp,symbol,bid,bid_qty,ask,ask_qty,last,volume,vwap,low,high,change,change_pct\n"
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
    Class extending BaseKrakenWS geared towards L2 feeds from the Kraken v2 API.
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
        append_book=True,
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
        self.updated = {s: False for s in symbols}
        self.books = {s: {} for s in symbols}
        self.auth = False
        self.trace = trace
        self.log_book_every = log_book_every
        self.log_bbo_every = log_bbo_every
        self.ask_prices = {s: [0.0] * self.depth for s in symbols}
        self.bid_prices = {s: [0.0] * self.depth for s in symbols}
        self.append_book = append_book
        self.count = 0
        self.output_directory = output_directory

    def _on_message(self, ws, message):
        response = json.loads(message)
        # self.count += 1

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
                symbol = data[0]["symbol"]
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

                self.books[symbol]["bids"] = {b["price"]: b["qty"] for b in bids}
                self.books[symbol]["asks"] = {a["price"]: a["qty"] for a in asks}

                self.bid_prices[symbol] = [b["price"] for b in bids]
                self.ask_prices[symbol] = [a["price"] for a in asks]

                self._book_checksum(ws, checksum, symbol)

            else:
                # Initalize a flag for if a given symbol's book has been updated.

                self.count += 1

                data = response["data"]
                symbol = data[0]["symbol"]
                self.updated[symbol] = True

                if ("bids" in response["data"][0].keys()) & (
                    "asks" in response["data"][0].keys()
                ):

                    bids = response["data"][0]["bids"]
                    asks = response["data"][0]["asks"]

                    bqty = [b["qty"] for b in bids]
                    aqty = [a["qty"] for a in asks]
                    new_bids = [
                        b["price"]
                        for b in bids
                        if b["price"] not in self.bid_prices[symbol]
                    ]
                    new_asks = [
                        a["price"]
                        for a in asks
                        if a["price"] not in self.ask_prices[symbol]
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
                                del self.books[symbol]["bids"][curr_price]
                                # Zero this bad price level in the self.prices array.
                                self.bid_prices[symbol].remove(curr_price)

                            # ADD GOOD PRICE LEVEL
                            else:
                                self.books[symbol]["bids"][curr_price] = bid["qty"]
                                if curr_price not in self.bid_prices[symbol]:
                                    self.bid_prices[symbol].append(curr_price)

                        # Fix: Sort the list directly
                        self.bid_prices[symbol].sort(reverse=True)

                        # HANDLE BOOKKEEPING OF THE DEPTH FOR MBP DATA
                        num_bid_levels = len(self.bid_prices[symbol])
                        if num_bid_levels == self.depth:
                            pass
                        elif num_bid_levels > self.depth:
                            # Fix: Get the prices to remove as a list instead of a slice
                            bad_prices = self.bid_prices[symbol][
                                self.depth :
                            ]  # Convert slice to list
                            for bp in bad_prices:
                                self.bid_prices[symbol].remove(bp)
                                del self.books[symbol]["bids"][bp]
                        else:
                            # FIXME handle this case more gracefully at some point.
                            ws.close()
                            raise ValueError(f"MBP Depth is lower than {self.depth}")

                    if len(asks) > 0:
                        for ask in asks:
                            curr_price = ask["price"]

                            # REMOVE BAD PRICE LEVEL
                            if ask["qty"] == 0:
                                # Remove the price level from the dictionary
                                del self.books[symbol]["asks"][curr_price]
                                # Zero this bad price level in the self.prices array.
                                self.ask_prices[symbol].remove(curr_price)

                            # ADD GOOD PRICE LEVEL
                            else:
                                self.books[symbol]["asks"][curr_price] = ask["qty"]
                                if curr_price not in self.ask_prices[symbol]:
                                    self.ask_prices[symbol].append(curr_price)

                        self.ask_prices[symbol].sort(reverse=False)

                        # HANDLE BOOKKEEPING OF THE DEPTH FOR MBP DATA
                        num_ask_levels = len(self.ask_prices[symbol])
                        if num_ask_levels == self.depth:
                            pass
                        elif num_ask_levels > self.depth:
                            # Fix: Get the prices to remove as a list instead of a slice
                            bad_prices = self.ask_prices[symbol][
                                self.depth :
                            ]  # Convert slice to list
                            for bp in bad_prices:
                                self.ask_prices[symbol].remove(bp)
                                del self.books[symbol]["asks"][bp]
                        else:
                            ws.close()
                            raise ValueError(f"MBP Depth is lower than {self.depth}")

                # Sort the self.bids and self.asks by their keys (prices)
                self.books[symbol]["bids"] = dict(
                    sorted(self.books[symbol]["bids"].items(), reverse=True)
                )
                self.books[symbol]["asks"] = dict(
                    sorted(self.books[symbol]["asks"].items(), reverse=False)
                )

                # FIXME Add counter for each symbol to consider logging separately.
                if self.count % self.log_book_every == 0:
                    if self.append_book:
                        for symbol in self.symbols:

                            if self.updated[symbol]:

                                output = True
                                full_L2_orderbook = {
                                    "b": self.books[symbol]["bids"],
                                    "a": self.books[symbol]["asks"],
                                }

                                aps = list(full_L2_orderbook["a"].keys())
                                avs = list(full_L2_orderbook["a"].values())
                                bps = list(full_L2_orderbook["b"].keys())
                                bvs = list(full_L2_orderbook["b"].values())

                                aps = [f"{ap:.9f}" for ap in aps]
                                avs = [f"{av:.9f}" for av in avs]
                                bps = [f"{bp:.9f}" for bp in bps]
                                bvs = [f"{bv:.9f}" for bv in bvs]

                                most_recent_timestamp = len(data) - 1

                                # NOTE TIME MAYBE NOT THE MOST ACCURATE
                                line = [str(data[most_recent_timestamp]["timestamp"])]
                                for i in range(self.depth):
                                    line.extend([aps[i], avs[i], bps[i], bvs[i]])

                                ssymbol = symbol.replace("/", "_")
                                if not os.path.exists(
                                    f"{self.output_directory}/L2_{ssymbol}_orderbook.csv"
                                ):
                                    with open(
                                        f"{self.output_directory}/L2_{ssymbol}_orderbook.csv",
                                        "w",
                                    ) as fil:

                                        labels = ["timestamp"]
                                        for i in range(self.depth):
                                            labels.extend(
                                                [
                                                    "ap" + str(i),
                                                    "av" + str(i),
                                                    "bp" + str(i),
                                                    "bv" + str(i),
                                                ]
                                            )

                                        fil.write(",".join(labels) + "\n")
                                        fil.write(",".join(line) + "\n")
                                else:
                                    with open(
                                        f"{self.output_directory}/L2_{ssymbol}_orderbook.csv",
                                        "a",
                                    ) as fil:
                                        fil.write(",".join(line) + "\n")

                            self.updated[symbol] = False
                    with open(
                        f"{self.output_directory}/L2_live_orderbooks.json", "w"
                    ) as fil:
                        json.dump(self.books, fil)
                else:
                    output = False

    def _book_checksum(self, ws, checksum, symbol):

        bid_keys = list(self.books[symbol]["bids"].keys())
        ask_keys = list(self.books[symbol]["asks"].keys())
        bid_vals = list(self.books[symbol]["bids"].values())
        ask_vals = list(self.books[symbol]["asks"].values())

        asksum = ""
        bidsum = ""
        for i in range(10):

            # String bid price, String bid ask
            sbp = str(bid_keys[i]).replace(".", "").lstrip("0")
            sap = str(ask_keys[i]).replace(".", "").lstrip("0")

            # String bid qty, String bid ask
            sbq = f"{self.books[symbol]['bids'][bid_keys[i]]:.8f}".replace(
                ".", ""
            ).lstrip("0")
            saq = f"{self.books[symbol]['asks'][ask_keys[i]]:.8f}".replace(
                ".", ""
            ).lstrip("0")

            sb = sbp + sbq
            sa = sap + saq

            asksum += sa
            bidsum += sb

        csum_string = asksum + bidsum
        kracked_checksum = CRC32(csum_string.encode("utf-8"))

        if checksum != kracked_checksum:
            ...
            # # FIXME ADD ERROR HANDLING HERE.
            # raise ValueError("CHECKSUM MISMATCH! BOOK IS INCONSISTENT!")

    def _on_open(self, ws):
        """
        Open message for Kraken L3 connection.
        """

        print("Kraken v2 Connection Opened.")
        # ws_token = self.get_ws_token(self.api_key, self.api_secret)

        subscription = {
            "method": "subscribe",
            "params": {
                "channel": "book",
                "symbol": self.symbols,
                "depth": self.depth,
            },
        }

        ws.send(json.dumps(subscription))


class KrakenL3(BaseKrakenWS):
    """
    Class extending BaseKrakenWS for the L3 feed from the Kraken v2 API.

    In brief, the class stores L3 data in memory until the threshold determined by
    log_ticks_every is reached. At this point, the data is written to a file in the
    output_directory with the name specified by out_file_name. All symbols are logged
    in the same file. One may wish to periodically convert the L3 data into aggregates,
    or migrate to a more suitable file format (e.g. parquet, HDf5, etc.)

    IMPORTANT NOTE: This is the only Kraken feed that requires authentication. You MUST provide
    an api_key and secret_key.

    Implements:
    -----------
    _on_open
    _on_message
    """

    def __init__(
        self,
        symbols,
        api_key=None,
        secret_key=None,
        trace=False,
        depth=10,
        out_file_name="L3_ticks.csv",
        log_ticks_every=100,
        log_for_webapp=False,  # FIXME does nothing
        output_directory=".",
    ):
        """
        Constructor for the KrakenL3 class.

        Parameters:
        -----------
            symbols: List[str] or str
                The symbols to subscribe to.
            api_key: str
                The user API key for the Kraken API.
            secret_key: str
                The user secret key for the Kraken API.
            trace: bool
                Whether to trace the websocket messages. Note these heavily clog the stdout.
            out_file_name: str
                The name of the file to log the L3 data to. All symbols are logged to
                the same file.
            log_ticks_every: int
                The number of incoming ticks before they are batch written to the desired
                output file.
            log_for_webapp: bool
                Whether to log information for the webapp. [Likely to be deprecated.]
            output_directory: str
                The directory to log the L3 data to.
        """

        self.tick_count = 0
        if type(symbols) == str:
            symbols = [symbols]

        self.symbols = symbols
        self.auth = True
        self.trace = trace
        self.api_key = api_key
        self.api_secret = secret_key
        self.depth = depth
        self.log_ticks_every = log_ticks_every
        self.out_file_name = out_file_name
        self.ticks = []
        self.output_directory = output_directory
        self.log_for_webapp = log_for_webapp

    def _on_message(self, ws, message):
        response = json.loads(message)

        my_time = datetime.datetime.now()

        if len(self.ticks) > self.log_ticks_every:
            if not os.path.exists(f"{self.output_directory}/{self.out_file_name}"):
                with open(f"{self.output_directory}/{self.out_file_name}", "w") as fil:
                    fil.write(
                        "side,ts_event,ts_recv,price,size,action,order_id,symbol\n"
                    )
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
                symbol = response["data"][0]["symbol"]
                if len(bids) > 0:
                    for bid in bids:
                        info = [
                            "b",  # Side
                            bid["timestamp"],  # Exchange Time
                            my_time,  # My Time
                            bid["limit_price"],  # Price
                            bid["order_qty"],  # Size
                            bid["event"],  # Action
                            bid["order_id"],  # OID
                            symbol,
                        ]
                        self.ticks.append(info)
                        self.tick_count += 1
                if len(asks) > 0:
                    for ask in asks:
                        info = [
                            "a",  # Side
                            ask["timestamp"],  # Exchange Time
                            my_time,  # My Time
                            ask["limit_price"],  # Price
                            ask["order_qty"],  # Size
                            ask["event"],  # Action
                            ask["order_id"],  # OID
                            symbol,
                        ]
                        self.ticks.append(info)
                        self.tick_count += 1
                if len(asks) == 0 and len(bids) == 0:
                    pass

        elif "data" in response.keys() and response["type"] == "snapshot":
            # Decide what to do with initial snapshot later
            pass

    def _on_open(self, ws):
        """
        Open message for Kraken L3 connection.
        """

        print("Kraken v2 Connection Opened.")
        ws_token = self.get_ws_token(self.api_key, self.api_secret)

        subscription = {
            "method": "subscribe",
            "params": {
                "channel": "level3",
                "symbol": self.symbols,
                "token": ws_token,
                "depth": self.depth,
            },
        }

        ws.send(json.dumps(subscription))


class KrakenOHLC(BaseKrakenWS):
    """
    Class extending BaseKrakenWS for the time-aggregated OHLC data from the Kraken v2 API.

    In brief, the class stores OHLC data and writes it to a file in the output_directory
    with the name OHLC.csv. All symbols are logged in the same file. Acceptable intervals are
    [1, 5, 15, 30, 60, 240, 1440, 10080, 21600] with units of minutes.

    IMPORTANT NOTE: This feed receives messages FAR more frequently than the required resolution
    of the bars. A concise way of dealing with this is to first load your data:

    df = pd.read_csv("OHLC.csv")

    Then, one should select for their desired symbol:

    df_symbol = df[df["symbol"] == "DOGE/USD"]

    Finally, one must aggregate only the FINAL row corresponding to a specific timestamp.

    df_symbol = df_symbol.groupby("timestamp").last().reset_index()

    Failure to do this will result in nonsense OHLC data. The benefit of including these in the output
    are that live plotting methods can allow for real-time (insofar as the API allows) candlestick updates
    for the current bar.

    Implements:
    -----------
    _on_message
    _on_open
    """

    def __init__(
        self,
        symbols,
        trace=False,
        interval=5,
        output_directory=".",
    ):
        """
        Constructor for the KrakenOHLC class.

        Parameters:
        -----------
            symbols: List[str] or str
                The symbols to subscribe to.
            trace: bool
                Whether to trace the websocket messages.
            interval: int
                The interval for the OHLC data in minutes. Acceptable values are
                [1, 5, 15, 30, 60, 240, 1440, 10080, 21600].
            output_directory: str
                The directory to log the OHLC data to.
        """

        all_int = [1, 5, 15, 30, 60, 240, 1440, 10080, 21600]

        # Ensure an acceptable interval is provided.
        assert interval in all_int, f"Choose interval from {all_int}"

        # Initialize the tick counter and symbols.
        self.tick_count = 0
        if type(symbols) == str:
            symbols = [symbols]

        # Initialize the symbols, authentication, tracing, and OHLC data storage.
        self.symbols = symbols
        self.auth = False
        self.trace = trace
        self.ticks = []
        self.interval = interval
        self.output_directory = output_directory

    def _on_message(self, ws, message):
        """
        Message handler for the OHLC feed.
        """
        response = json.loads(message)

        reponse_keys = list(response.keys())

        if "channel" in reponse_keys:

            # Main case for handling the OHLC data from Kraken
            if response["channel"] == "ohlc":

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

                    if not os.path.exists(f"{self.output_directory}/OHLC.csv"):
                        with open(f"{self.output_directory}/OHLC.csv", "a") as fil:
                            fil.write(
                                "timestamp,open,high,low,close,volume,vwap,trades,tstart,ttrue\n"
                            )
                            fil.write(",".join(info) + "\n")

                    with open(f"{self.output_directory}/OHLC.csv", "a") as fil:
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
                        if not os.path.exists(f"{self.output_directory}/OHLC.csv"):
                            with open(f"{self.output_directory}/OHLC.csv", "w") as fil:
                                fil.write(
                                    "timestamp,symbol,open,high,low,close,volume,vwap,trades,tstart,ttrue\n"
                                )
                        else:
                            with open(f"{self.output_directory}/OHLC.csv", "a") as fil:
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

    def __init__(
        self,
        symbols,
        api_key=None,
        secret_key=None,
        trace=False,
        log_trades_every=100,
        output_directory=".",
    ):
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
            if response["channel"] == "trade":
                if response["type"] in ["update", "snapshot"]:
                    filled_trades = response["data"]
                    for trade in filled_trades:
                        ts_event = trade["timestamp"]
                        symbol = trade["symbol"]
                        price = str(trade["price"])
                        qty = str(trade["qty"])
                        side = trade["side"]
                        ord_type = trade["ord_type"]
                        trade_id = str(trade["trade_id"])
                        self.all_trades.append(
                            [ts_event, symbol, price, qty, side, ord_type, trade_id]
                        )

                elif response["type"] == "snapshot":
                    pass
            elif response["channel"] in ["heartbeat", "status", "subscribe"]:
                pass

        if len(self.all_trades) >= self.log_trades_every:

            if not os.path.exists(f"{self.output_directory}/trades.csv"):
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
            "params": {"channel": "trade", "symbol": self.symbols, "snapshot": True},
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
                unique_quotes = []

                header_pairs = pairs[0].keys()
                header_assets = assets[0].keys()

                keys = [
                    "symbol",
                    "base",
                    "quote",
                    "status",
                    "qty_precision",
                    "qty_increment",
                    "price_precision",
                    "cost_precision",
                    "marginable",
                    "has_index",
                    "cost_min",
                    "margin_initial",
                    "position_limit_long",
                    "position_limit_short",
                    "tick_size",
                    "price_increment",
                    "qty_min",
                ]

                pair_info = []
                for pair in pairs:
                    for key in keys:
                        if key not in pair.keys():
                            pair[key] = "None"

                    pair_info.append([str(pair[key]) for key in keys])

                asset_info = [[str(x) for x in a.values()] for a in assets]

                with open(f"{self.output_directory}/kraken_pairs.csv", "w") as fil:
                    fil.write(",".join(keys) + "\n")
                    for line in pair_info:
                        fil.write(",".join(line) + "\n")

                with open(f"{self.output_directory}/kraken_assets.csv", "w") as fil:
                    fil.write(",".join(header_assets) + "\n")
                    for line in asset_info:
                        fil.write(",".join(line) + "\n")

                ws.close()
                exit(1)

    def _on_open(self, ws):

        print("Kraken v2 Connection Opened.")
        # ws_token = self.get_ws_token(self.api_key, self.api_secret)

        subscription = {
            "method": "subscribe",
            "params": {"channel": "instrument"},
            "req_id": 79,
        }

        ws.send(json.dumps(subscription))
