from kracked.core import BaseKrakenWS
from zlib import crc32 as CRC32
import numpy as np
import toml, json, os
import datetime

import logging

logging.basicConfig(level=logging.DEBUG)


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
        out_file_name="output.csv",
        write_every=100,
        log_for_webapp=False,
        log_dir=".",
    ):

        self.tick_count = 0
        if type(symbols) == str:
            symbols = [symbols]

        self.symbols = symbols
        self.auth = True
        self.trace = trace
        self.api_key = api_key
        self.api_secret = secret_key
        self.write_every = write_every
        self.out_file_name = out_file_name
        self.ticks = []
        self.log_for_webapp = log_for_webapp

    def _on_message(self, ws, message):
        response = json.loads(message)
        if len(self.ticks) > self.write_every:
            with open(self.out_file_name, "a+") as fil:
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
        depth=10,
        output_dir=".",
        log_every=100,
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
        self.api_key = api_key
        self.api_secret = secret_key
        self.log_every = log_every
        self.log_bbo_every = log_bbo_every
        self.ask_prices = [0.0] * self.depth
        self.bid_prices = [0.0] * self.depth
        self.ask_volumes = np.zeros(self.depth)
        self.bid_volumes = np.zeros(self.depth)
        self.count = 0
        self.output_dir = output_dir

    def _on_message(self, ws, message):
        response = json.loads(message)
        self.count += 1

        if "method" in response.keys():
            # Pass these.
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

                if self.count % self.log_every:
                    output = True
                    full_L2_orderbook = {"b": self.bids, "a": self.asks}

                    with open(f"{self.output_dir}/L2_orderbook.json", "w") as fil:
                        json.dump(full_L2_orderbook, fil)
                else:
                    output = False

                if self.count % self.log_bbo_every == 0:
                    print("here")
                    if not os.path.exists("L1_BBO.csv"):
                        with open(f"{self.output_dir}/L1_BBO.csv", "w") as fil:
                            fil.write("timestamp,bbo,bao\n")
                    else:
                        with open("L1_BBO.csv", "a") as fil:
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
        ws_token = self.get_ws_token(self.api_key, self.api_secret)

        subscription = {
            "method": "subscribe",
            "params": {"channel": "book", "symbol": self.symbols, "token": ws_token},
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
        self.api_key = api_key
        self.api_secret = secret_key
        self.ticks = []
        self.interval = interval
        self.output_directory = output_directory

    def _on_message(self, ws, message):
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
                            with open(f"{self.output_directory}/ohlc.csv", "a") as fil:
                                fil.write(
                                    "tend,open,high,low,close,volume,vwap,trades,tstart,ttrue\n"
                                )

                        with open(f"{self.output_directory}/ohlc.csv", "a") as fil:
                            fil.write(",".join(info) + "\n")

            elif response["channel"] in ["heartbeat", "status", "subscribe"]:
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
                "channel": "ohlc",
                "symbol": self.symbols,
                # "token": ws_token
                "interval": self.interval,
            },
        }

        ws.send(json.dumps(subscription))


if __name__ == "__main__":
    with open(f"/home/alg/.api.toml", "r") as fil:
        data = toml.load(fil)
    api_key = data["kraken_api"]
    api_secret = data["kraken_sec"]

    # l2feed = KrakenL2("DOGE/USD",
    # trace=False,
    # api_key=api_key,
    # secret_key=api_secret)
    # l2feed.launch()
    # l3feed.launch()

    ohlcfeed = KrakenOHLC(
        "DOGE/USD", interval=1, trace=False, api_key=api_key, secret_key=api_secret
    )

    ohlcfeed.launch()
