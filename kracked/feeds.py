from kracked.core import BaseKrakenWS
from zlib import crc32 as CRC32
import numpy as np
import toml, json

import logging

logging.basicConfig(level=logging.DEBUG)


class KrakenL3(BaseKrakenWS):
    """
    Class extending BaseKrakenWS geared towards L3 feeds from the Kraken v2 API.
    """

    def __init__(self, symbols, api_key=None, secret_key=None,trace=False,
                 out_file_name="output.csv", write_every=100, log_for_webapp=False,
                  log_dir="."):

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
                    fil.write(",".join(tick)+"\n")
            self.ticks = []
            self.tick_count = 0

        # print(response)
        if 'data' in response.keys() and response['type'] != 'snapshot':
            assert len(response['data']) == 1 , "Haven't seen this response before"
            if 'bids' in response['data'][0].keys() and 'asks' in response['data'][0].keys():
                bids = response['data'][0]['bids']
                asks = response['data'][0]['asks']
                if len(bids) > 0:
                    for bid in bids:
                        print(bid)
                        info = ['b',                # Side
                                bid['timestamp'],   # Time
                                bid['limit_price'], # Price
                                bid['order_qty'],   # Size
                                bid['event'],       # Event
                                bid['order_id'],    # OID
                                ]
                        self.ticks.append(info)
                        self.tick_count += 1
                if len(asks) > 0:
                    for ask in asks:
                        info = ['a',                # Side
                                ask['timestamp'],   # Time
                                ask['limit_price'], # Price
                                ask['order_qty'],   # Size
                                ask['event'],       # Event
                                ask['order_id'],    # OID
                                ]
                        self.ticks.append(info)
                        self.tick_count += 1
                if len(asks) == 0 and len(bids) == 0:
                    print(response)


        elif 'data' in response.keys() and response['type'] == 'snapshot':
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
            "params": {
                "channel": "level3",
                "symbol": self.symbols,
                "token": ws_token
            }
        }

        ws.send(json.dumps(subscription)) 


class KrakenL2(BaseKrakenWS):

    """
    Class extending BaseKrakenWS geared towards L3 feeds from the Kraken v2 API.
    """

    def __init__(self, symbols, api_key=None, secret_key=None,trace=False,
                 depth = 10,
                 out_file_name="output.csv", log_every=5):

        assert depth in [10, 25, 100, 500, 1000] , "Depths allowed: 10, 25, 100, 500, 1000"

        if type(symbols) == str:
            symbols = [symbols]

        self.depth = depth
        self.symbols = symbols
        self.auth = False
        self.trace = trace
        self.api_key = api_key
        self.api_secret = secret_key
        self.log_every = log_every
        self.out_file_name = out_file_name
        self.ask_prices = [0.0]*self.depth
        self.bid_prices = [0.0]*self.depth
        self.ask_volumes = np.zeros(self.depth)
        self.bid_volumes = np.zeros(self.depth)
        self.count = 0


    def _on_message(self, ws, message):
        response = json.loads(message)
        self.count += 1

        if 'method' in response.keys():
            # Pass these.
            pass

        if 'channel' in response.keys():

            # Skip heartbeats and status messges.
            if response['channel'] == 'heartbeat' or response['channel'] == 'status':
                pass

            # SNAPSHOTS RESET THE ORDERBOOK FROM OUR MANUAL UPDATE PROCESS.
            elif response['type'] == 'snapshot':
                # Pull data
                data = response['data']
                if len(data) != 1:
                    raise ValueError("Data longer than expected")
                data = data[0]
                checksum = data['checksum']

                # Bids -> Asserts snapshot fills the orderbook (w.r.t self.depth)
                bids = data['bids']
                assert len(bids) == self.depth , "Snapshot should be full book refresh."


                # Asks -> Asserts snapshot fills the orderbook (w.r.t self.depth)
                asks = data['asks']
                assert len(asks) == self.depth , "Snapshot should be full book refresh."

                self.bids = {b['price']:b['qty'] for b in bids}
                self.asks = {a['price']:a['qty'] for a in asks}

                self.bid_prices = [b['price'] for b in bids]
                self.ask_prices = [a['price'] for a in asks]

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

                if self.count % self.log_every:
                    output = True
                    full_L2_orderbook = {'b': self.bids,
                                         'a': self.asks}
                    with open("L2_orderbook.json", "w") as fil:
                        json.dump(full_L2_orderbook, fil)


                else:
                    output = False

                if output:
                    print("\n")
                    print("ASK INFO")
                    print(self.ask_prices)
                    print(self.asks)

                data = response['data']
                if ('bids' in response['data'][0].keys()) & \
                   ('asks' in response['data'][0].keys()):

                    bids = response['data'][0]['bids']
                    asks = response['data'][0]['asks']

                    bqty = [b['qty'] for b in bids]
                    aqty = [a['qty'] for a in asks]
                    new_bids = [b['price'] for b in bids if b['price'] not in self.bid_prices]
                    new_asks = [a['price'] for a in asks if a['price'] not in self.ask_prices]

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
                            curr_price = bid['price']

                            # REMOVE BAD PRICE LEVEL
                            if bid['qty'] == 0:
                                # Remove the price level from the dictionary
                                del self.bids[curr_price]
                                # Zero this bad price level in the self.prices array.
                                self.bid_prices.remove(curr_price)

                            # ADD GOOD PRICE LEVEL
                            else:
                                self.bids[curr_price] = bid['qty']
                                if curr_price not in self.bid_prices:
                                    self.bid_prices.append(curr_price)

                        self.bid_prices.sort(reverse=True)

                        # HANDLE BOOKKEEPING OF THE DEPTH FOR MBP DATA
                        num_bid_levels = len(self.bid_prices)
                        if num_bid_levels == self.depth:
                            pass
                        elif num_bid_levels > self.depth:
                            # Remove all other price levels
                            bad_prices = self.bid_prices[self.depth:]
                            for bp in bad_prices:
                                self.bid_prices.remove(bp)
                                del self.bids[bp]
                        else:
                            ws.close()
                            raise ValueError(f"MBP Depth is lower than {self.depth}")

                    if len(asks) > 0:
                        for ask in asks:
                            curr_price = ask['price']

                            # REMOVE BAD PRICE LEVEL
                            if ask['qty'] == 0:
                                # Remove the price level from the dictionary
                                del self.asks[curr_price]
                                # Zero this bad price level in the self.prices array.
                                self.ask_prices.remove(curr_price)

                            # ADD GOOD PRICE LEVEL
                            else:
                                self.asks[curr_price] = ask['qty']
                                if curr_price not in self.ask_prices:
                                    self.ask_prices.append(curr_price)

                        self.ask_prices.sort(reverse=False)

                        # HANDLE BOOKKEEPING OF THE DEPTH FOR MBP DATA
                        num_ask_levels = len(self.ask_prices)
                        if num_ask_levels == self.depth:
                            pass
                        elif num_ask_levels > self.depth:
                            # Remove all other price levels
                            bad_prices = self.ask_prices[self.depth:]
                            for bp in bad_prices:
                                self.ask_prices.remove(bp)
                                del self.asks[bp]
                        else:
                            ws.close()
                            raise ValueError(f"MBP Depth is lower than {self.depth}")



    def _book_checksum(self, ws, checksum):

        bid_keys = list(self.bids.keys())
        ask_keys = list(self.asks.keys())
        bid_vals = list(self.bids.values())
        ask_vals = list(self.asks.values())

        # print("Bids")
        # print(bid_keys)
        # print(bid_vals)
        # print("Asks")
        # print(ask_keys)
        # print(ask_vals)

        asksum = ""
        bidsum = ""
        for i in range(10):

            # String bid price, String bid ask
            sbp = str(bid_keys[i]).replace(".", "").lstrip("0")
            sap = str(ask_keys[i]).replace(".", "").lstrip("0")

            # String bid qty, String bid ask
            sbq = f"{self.bids[bid_keys[i]]:.8f}".replace(".", "").lstrip("0")
            saq = f"{self.asks[ask_keys[i]]:.8f}".replace(".", "").lstrip("0")

            sb = sbp+sbq
            sa = sap+saq

            asksum += sa
            bidsum += sb

        csum_string = asksum + bidsum
        kracked_checksum = CRC32(csum_string.encode('utf-8'))

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
            "params": {
                "channel": "book",
                "symbol": self.symbols,
                "token": ws_token
            }
        }

        ws.send(json.dumps(subscription)) 


with open(f"/home/alg/.api.toml", "r") as fil:
    data = toml.load(fil)
api_key = data['kraken_api']
api_secret = data['kraken_sec']


# l3feed = KrakenL3("BTC/USD",
#                   trace=False, 
#                   api_key=api_key,
#                   secret_key=api_secret)

l2feed = KrakenL2("BTC/USD",
                  trace=False, 
                  api_key=api_key,
                  secret_key=api_secret)


# l3feed.launch()
l2feed.launch()