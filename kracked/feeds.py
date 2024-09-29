from kracked.core import BaseKrakenWS
import toml, json

class KrakenL3(BaseKrakenWS):
    """
    Class extending BaseKrakenWS geared towards L3 feeds from the Kraken v2 API.
    """

    def __init__(self, symbols, api_key=None, secret_key=None,trace=False,
                 out_file_name="output.csv", write_every=100):

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
                 out_file_name="output.csv", write_every=100):

        assert depth in [10, 25, 100, 500, 1000] , "Depths allowed: 10, 25, 100, 500, 1000"

        if type(symbols) == str:
            symbols = [symbols]

        self.depth = depth
        self.symbols = symbols
        self.auth = False
        self.trace = trace
        self.api_key = api_key
        self.api_secret = secret_key
        self.write_every = write_every
        self.out_file_name = out_file_name
        self.ask_prices = []
        self.bid_prices = []
        self.ask_volumes = []
        self.bid_volumes = []


    def _on_message(self, ws, message):
        response = json.loads(message)
        print(response)
        exit(1)
        if response['channel'] == 'heartbeat':
            pass
        else:
            data = response['data']
            if 'bids' in response['data'][0].keys() and 'asks' in response['data'][0].keys():
                bids = response['data'][0]['bids']
                asks = response['data'][0]['asks']
                if len(bids) > 0:
                    for bid in bids:
                        print(bid)
                if len(asks) > 0:
                    for bid in bids:
                        print(bid)


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


l3feed = KrakenL3("BTC/USD",
                  trace=False, 
                  api_key=api_key,
                  secret_key=api_secret)

l2feed = KrakenL2("BTC/USD",
                  trace=False, 
                  api_key=api_key,
                  secret_key=api_secret)


# l3feed.launch()
l2feed.launch()