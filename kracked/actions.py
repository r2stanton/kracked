import json, logging, threading, time, toml
from kracked.core import BaseKrakenWS
from datetime import datetime, timezone
import ccxt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KrakenExecutions(BaseKrakenWS):
    def __init__(self, api_key=None, secret_key=None, trace=False, session_id=1):
        self.ws_thread = None
        self.connected = threading.Event()
        self.ws = None
        self.auth = True
        self.trace = trace
        self.api_key = api_key
        self.api_secret = secret_key
        self.session_id = session_id

        self.balances = {}

        self.action_number = 0

        self.order_info = {'order_id': [],
                           'success': [],
                           'local_time': [],
                           'time_in': [],
                           'time_out': [],
                           'original_order_id': [],
                           'amend_id': []}

        with open(f"session_id_{session_id}.csv", "w") as fil:
            fil.write(",".join(self.order_info.keys()) + "\n")

    def _on_open(self, ws):
        print("Kraken v2 Connection Opened.")
        self.ws = ws
        print("webscoket info within KrakenExecutions._on_open")
        print(self.ws)
        self.connected.set()
        self.get_balances()

    def _on_message(self, ws, message):
        print(message)
        # Needs to be implemented
        response = json.loads(message)
        # print(response)
        if "channel" in response.keys():
            if response["channel"] == "status":
                ...
            elif response["channel"] == "balances":
                data = response["data"]
                for d in data:
                    info_dict = {'asset_class':d['asset_class'], 'balance':d['balance'], 'wallets': d['wallets']}
                    self.balances[d['asset']] = info_dict
        else:
            if response["method"] == "add_order":
                order_id = response["result"]["order_id"]
                success = response["success"]
                time_in = response["time_in"]
                time_out = response["time_out"]

                self.order_info['order_id'].append(order_id)
                self.order_info['success'].append(success)
                self.order_info['time_in'].append(time_in)
                self.order_info['time_out'].append(time_out)
                self.order_info['original_order_id'].append(None)
                self.order_info['amend_id'].append(None)


                with open(f"session_id_{self.session_id}.csv", "a") as fil:
                    #Write action number'th element of each list in the order_info dictionary
                    fil.write(",".join([str(self.order_info[key][self.action_number]) for key in self.order_info.keys()]) + "\n")

                self.action_number += 1

    def _on_error(self, ws, error):
        print(error)
        self.connected.clear()

    def _on_close(self, ws, close_status_code, close_msg):
        print(close_msg)
        print(f"WebSocket closed. Status code: {close_status_code}, Message: {close_msg}")
        self.connected.clear()

    def start_websocket(self):
        if self.ws_thread and self.ws_thread.is_alive():
            print("WebSocket is already running.")
            return

        self.ws_thread = threading.Thread(target=self.launch)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        print("WebSocket thread started.")

    def stop_websocket(self):
        if not self.connected.is_set():
            print("WebSocket is not connected.")
            return

        if self.ws:
            self.ws.close()
        if self.ws_thread:
            self.ws_thread.join(timeout=5)
        print("WebSocket connection closed.")

    def cancel_order(self, order_id):
        if not self.connected.is_set():
            print("WebSocket is not connected. Cannot submit order.")
            return

        # This logic is for cancelling all open orders *WHICH WERE CREATED BY THIS SESSION*
        if order_id == "all":
            message = {
                "method": "cancel_order",
                "params": {
                },
            }
            message["params"]["token"] = self.get_ws_token(self.api_key, self.api_secret)

            for oid in self.order_info['order_id']:
                message["params"]["order_id"] = [oid]
                self.ws.send(json.dumps(message))
            return

        else:
            if type(order_id) == str or type(order_id) == int:
                order_id = [order_id]
            else:
                assert type(order_id) == list


            message = {
                "method": "cancel_order",
                "params": {
                    "order_id": order_id
                },
            }

            message["params"]["token"] = self.get_ws_token(self.api_key, self.api_secret)

            self.ws.send(json.dumps(message))

    def add_order(self, 
                  order_type=None,
                  side=None,
                  order_qty=None,
                  symbol=None,
                  limit_price=None,
                  limit_price_type="quote",
                  trigger_params=None,
                  time_in_force='gtc',
                  margin=False,
                  post_only=False,
                  reduce_only=False,
                  effective_time=None,
                  expire_time=None,
                  deadline=None,
                  cl_ord_id=None,
                  order_userref=None,
                  conditional=None,
                  conditional_params=None,
                  display_qty=None,
                  fee_preference=None,
                  no_mpp=False,
                  stp_type="cancel_newest",
                  cash_order_qty=None,
                  validate=False,
                  sender_sub_id=None,
                  req_id=None,
    ):
        """
        Function for creating orders in the Kraken Exchange. Not all parameters are
        required for all order types. Be sure you understand the necessary requirements
        for each order type that you seek to trade. Parameter names are all the same as 
        those defined in the Kraken API. See https://docs.kraken.com/api/docs/websocket-v2/add_order
        for more information.

        Parameters:
        -----------
        order_type: str
            The type of order to be created. Must be one of the following:
                "market", "limit",
                "stop-loss", "stop-loss-limit",
                "take-profit", "take-profit-limit",
                "trailing-stop", "trailing-stop-limit",
                "iceberg".

        side: str
            The side of the order.
        order_qty: float
            The quantity of the order.
        symbol: str
            The symbol for the order.
        limit_price: float
            The price of the order.
        time_in_force: str
            The time in force of the order. Must be one of the following:
                "gtc" -> Good Till Cancelled.
                "gtd" -> Good Till Date. See expire_time parameter.
                "ioc" -> Immediate or Cancel. Cancels any unfilled portion of the order
                        upon receipt and implementation by the exchange.
        trigger_params: dict
            The parameters for the trigger of the applicable order types:
            stop-loss orders, take-profit orders, and  trailing-stop orders.

            Relevant parameters include:
                "reference" -> The reference price for the trigger, "last" or index". Default is "last".
                "price" -> The price at which the trigger should fire.
                "price_type" -> Type of price definition.
                    "static" -> The price is fixed.
                    "pct" -> Percentage offset from the index.
                    "quote" -> Notional offset from reference price in quote currency. E.g.
                            some fixed number of dollars from the last price.

        margin: bool
            Whether the order is a margin order.
        post_only: bool
            Whether the order is a post only order.
        reduce_only: bool
            Whether the order is a reduce only order.
        effective_time: str
            The effective time of the order.
        expire_time: str
            The expire time of the order.
        deadline: str
            The deadline of the order.
        cl_ord_id: str
            The client order id of the order.
        order_userref: str
            The user reference of the order.

            
        """

        if fee_preference is not None and side is not None:
            if side == "buy":
                fee_preference = "quote"
            elif side == "sell":
                fee_preference = "base"


        if not self.connected.is_set():
            print("WebSocket is not connected. Cannot submit order.")
            return

        order_types = ["market", "limit", "stop-loss", "stop-loss-limit", "take-profit", "take-profit-limit", "trailing-stop", "trailing-stop-limit", "iceberg"]

        assert order_type in order_types , f"Order type must be one of the following: {order_types}"

        if order_type == "market":
            order_type = "market"
        elif order_type == "limit":
            message = {
                "method": "add_order",
                "params": {
                    "order_type": order_type,
                    "side": side,
                    "order_qty": order_qty,
                    "symbol": symbol,
                    "limit_price": limit_price,
                    "time_in_force": time_in_force,
                    "validate": validate,
                }
            }
        elif order_type == "stop-loss":
            message = {
                "method": "add_order",
                "params": {
                    "order_type": order_type,
                    "side": "sell",
                    "order_qty": order_qty,
                    "symbol": symbol,
                    "limit_price": limit_price,
                    "time_in_force": time_in_force,
                    "triggers": trigger_params,
                    "validate": validate,
                }
            }
        elif order_type == "stop-loss-limit":
            order_type = "stop-loss-limit"
        elif order_type == "take-profit":
            order_type = "take-profit"
        elif order_type == "take-profit-limit":
            order_type = "take-profit-limit"
        elif order_type == "trailing-stop":
            order_type = "trailing-stop"
        elif order_type == "trailing-stop-limit":
            order_type = "trailing-stop-limit"
        elif order_type == "iceberg":
            order_type = "iceberg"


        message["params"]["token"] = self.get_ws_token(self.api_key, self.api_secret)

        current_time = datetime.now(timezone.utc)
        formatted_time = current_time.isoformat(timespec='microseconds')

        self.order_info['local_time'].append(formatted_time)
        self.ws.send(json.dumps(message))

    def get_balances(self):
        message = {
            "method": "subscribe",
            "params": {
                "channel": "balances",
            }
        }
        message["params"]["token"] = self.get_ws_token(self.api_key, self.api_secret)

        self.ws.send(json.dumps(message))

class KrakenPortfolio:
    def __init__(self, kraken_ccxt):
        self.kraken_ccxt = kraken_ccxt

        self.log_open_orders()

    def log_open_orders(self, static_exchange=None):
        
        all_open_orders = self.kraken_ccxt.fetch_open_orders()
        relevant_order_info = []
        unique_symbols = []

        for ord in all_open_orders:
            oid = ord['id']
            oinfo = ord['info']
            
            symbol = ord['symbol']
            if symbol not in unique_symbols:
                unique_symbols.append(symbol)

            status = ord['status']
            open_time = ord['timestamp']
            price = ord['price']
            qty = ord['amount']
            qty_exec = ord['filled']
            side = ord['side']

            relevant_order_info.append({'id': oid,
                                        'symbol': symbol,
                                        'status': status,
                                        'open_time': open_time,
                                        'price': price,
                                        'qty': qty,
                                        'qty_exec': qty_exec,
                                        'side': side})


        self.open_orders = {s:[] for s in unique_symbols}

        for o in relevant_order_info:
            self.open_orders[o['symbol']].append(o)

        
def add_order(kraken_ccxt, order_type, symbol, side, amount, price, 
              validate=False, time_in_force='gtc', displayvol=None,
              ):

    try:
        result = kraken_ccxt.create_order(symbol,
                                        order_type,
                                        side,
                                        amount,
                                        price,
                                        params={"validate": validate,
                                                "time_in_force": time_in_force})
        print(f"Order placed successfully: {result}")
    except ccxt.NetworkError as e:
        print(f"Network error occurred: {e}")
        return {}
    except ccxt.ExchangeError as e:
        print(f"Exchange error occurred: {e}")
        return {}
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {}

    return result

def cancel_order(kraken_ccxt, order_id):

    try:
        response = kraken_ccxt.cancel_order(order_id)
    except ccxt.NetworkError as e:
        print(f"Network error occurred: {e}")
        return {}
    except ccxt.ExchangeError as e:
        print(f"Exchange error occurred: {e}")
        return {}
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return response

def cancel_all_by_symbol(kraken_ccxt, symbol, open_orders=None):

    if open_orders is None:
        kp = KrakenPortfolio(kraken_ccxt)
        open_orders = kp.open_orders

    if symbol not in open_orders.keys():
        print(f"No open orders for {symbol}")
        return None

    open_orders_for_symbol = open_orders[symbol]
    # print(open_orders_for_symbol)

    for o in open_orders_for_symbol:
        cancel_order(kraken_ccxt, o['id'])

if __name__ == "__main__":
    with open("/home/alg/.api.toml", "r") as fil:
        data = toml.load(fil)
    api_key = data["kraken_api"]
    api_secret = data["kraken_sec"]

    exec = KrakenExecutions(api_key=api_key, secret_key=api_secret, trace=False, session_id=10)

    exec.start_websocket()

    # Wait for the WebSocket to connect
    if not exec.connected.wait(timeout=30):
        print("Failed to establish WebSocket connection.")
        exit(1)

    print("WebSocket connection established successfully.")

    # Now it's safe to add an order
    exec.add_order(
        order_type="limit",
        symbol="BTC/USD",
        side="buy",
        order_qty=0.001,
        limit_price=66000,
        time_in_force='gtc'
    )

    # try:
    #     # Keep the main thread running
    #     while True:
    #         time.sleep(1)
    # except KeyboardInterrupt:
    #     print("Interrupted by user. Closing WebSocket...")
    #     exec.stop_websocket()
