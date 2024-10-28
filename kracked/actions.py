import json, threading, time, toml
from kracked.core import BaseKrakenWS
from datetime import datetime, timezone

import warnings
import ccxt


class KrakenPortfolio:
    """

    Thin wrapper around ccxt functionalities with some additional niceties specific
    to this package and the Kraken API.

    """
    def __init__(self, kraken_ccxt):
        self.kraken_ccxt = kraken_ccxt
        
        # After these functions:

        # self.open_orders is set.
        self.get_open_orders()

        # self.balances is set.
        self.get_balances()

    def get_balances(self):

        total_bals = self.kraken_ccxt.fetch_balance()['total']

        self.balances = total_bals

    def get_open_orders(self, static_exchange=None):
        
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
        
def add_order(kraken_ccxt, order_type, symbol, side, amount, 
              price=None, validate=False, time_in_force='gtc', displayvol=None,
              safe_mode=True, trigger="last", leverage=None, post_only=False,
              action_price=None
              ):


    # Checks that the input are valid and that the requested order has the necessary 
    # information to be carried out for the user.
    if safe_mode:
        if order_type not in  ["market", "stop-loss", "take-profit"]:
            assert price is not None, "Price must be provided for non-market order types."
        if order_type in ["take-profit-limit",
                          "stop-loss-limit"]:
            assert action_price is not None, "Action price must be provided for take-profit and stop-loss order types."
        if order_type != "sell" and type(amount) == str:
            raise ValueError("Amount must be a number for buy orders.")
        if amount == "all":
            warnings.warn("Caution, requiring CCXT to fetch your balances is a slow approach "
                        "don't use for HFT strategies.")
            amount = kraken_ccxt.fetch_balance()['total'][symbol.split("/")[0]]

    try:
        if order_type == "market":
            result = kraken_ccxt.create_order(symbol,
                                            order_type,
                                            side,
                                            amount,
                                            price,
                                            params={"validate": validate,
                                                    "time_in_force": time_in_force})
            print(f"Order placed successfully: {result}")
        elif order_type == "limit":
            result = kraken_ccxt.create_order(symbol,
                                              order_type,
                                              side,
                                              amount,
                                              price,
                                              params={"validate": validate,
                                                      "time_in_force": time_in_force,
                                                      "postOnly": post_only})

        elif order_type in ["take-profit-limit", "stop-loss-limit"]:
            print(price)
            result = kraken_ccxt.create_order(symbol,
                                              order_type,
                                              side,
                                              amount,
                                              price,
                                              params={"validate": validate,
                                                      "time_in_force": time_in_force,
                                                      "postOnly": post_only,
                                                      "price2": .19})
            print(f"Order placed successfully: {result}")

        elif order_type in ["take-profit", "stop-loss"]:
            print("Here in take-profit or stop-loss")
            result = kraken_ccxt.create_order(symbol,
                                              order_type,
                                              side,
                                              amount,
                                              price)  # Set price to None
                                            #   params={"triggerPrice": price})  # Use price as triggerPrice
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

def cancel_all(kraken_ccxt, open_orders=None):


    if open_orders is None:
        kp = KrakenPortfolio(kraken_ccxt)
        open_orders = kp.open_orders

    for symbol in open_orders.keys():
        open_orders_for_symbol = open_orders[symbol]
        # print(open_orders_for_symbol)

        for o in open_orders_for_symbol:
            cancel_order(kraken_ccxt, o['id'])


if __name__ == "__main__":
    with open("/home/alg/.api.toml", "r") as fil:
        data = toml.load(fil)
    api_key = data["kraken_api"]
    api_secret = data["kraken_sec"]

    kraken_ccxt = ccxt.kraken({
        'apiKey': api_key,
        'secret': api_secret,
    })

    # test_real_order_placements = True
    # test_cancel_all = True
    test_cancel_symbol = True

    test_real_order_placements = False
    test_cancel_all = False
    # test_cancel_symbol = False

    if test_real_order_placements:
        # Now it's safe to add an order
        add_order(kraken_ccxt, 
                "limit", 
                "DOGE/USD", 
                "buy", 
                30.5, 
                price=.130, 
                validate=False, 
                time_in_force='gtc', 
                displayvol=None,
        )
        add_order(kraken_ccxt, 
                "limit", 
                "BTC/USD", 
                "buy", 
                .001, 
                price=67500, 
                validate=False, 
                time_in_force='gtc', 
                displayvol=None,
        )
        add_order(kraken_ccxt, 
                "limit", 
                "DOGE/USD", 
                "buy", 
                30.5, 
                price=.138, 
                validate=False, 
                time_in_force='gtc', 
                displayvol=None,
        )


    if test_cancel_all:
        cancel_all(kraken_ccxt)

    if test_cancel_symbol:
        cancel_all_by_symbol(kraken_ccxt, "DOGE/USD")

    # kraken_ccxt, order_type, symbol, side, amount, price, 
    #               validate=False, time_in_force='gtc', displayvol=None,
