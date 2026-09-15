import warnings

from kracked.rest import KrakenREST, KrakenRESTError


def _as_rest(client):
    if isinstance(client, KrakenREST):
        return client
    raise TypeError(
        "Expected a KrakenREST instance. kracked no longer uses ccxt; "
        "construct one with KrakenREST(api_key=..., api_secret=...)."
    )


class KrakenPortfolio:
    """Thin wrapper around Kraken private REST for balances and open orders."""

    def __init__(self, kraken_rest):
        self.kraken_rest = _as_rest(kraken_rest)
        self.get_open_orders()
        self.get_balances()

    def get_balances(self):
        self.balances = self.kraken_rest.get_balance()

    def get_open_orders(self, static_exchange=None):
        result = self.kraken_rest.get_open_orders()
        open_orders = result.get("open") or {}
        relevant_order_info = []
        unique_symbols = []

        for oid, ord_info in open_orders.items():
            descr = ord_info.get("descr") or {}
            pair = descr.get("pair", "")
            symbol = self.kraken_rest.display_symbol(pair)
            if symbol not in unique_symbols:
                unique_symbols.append(symbol)

            relevant_order_info.append(
                {
                    "id": oid,
                    "symbol": symbol,
                    "status": ord_info.get("status"),
                    "open_time": ord_info.get("opentm"),
                    "price": descr.get("price") or ord_info.get("price"),
                    "qty": ord_info.get("vol"),
                    "qty_exec": ord_info.get("vol_exec"),
                    "side": descr.get("type"),
                }
            )

        self.open_orders = {s: [] for s in unique_symbols}

        for o in relevant_order_info:
            self.open_orders[o["symbol"]].append(o)


def add_order(
    kraken_rest,
    order_type,
    symbol,
    side,
    amount,
    price=None,
    validate=False,
    time_in_force="gtc",
    displayvol=None,
    safe_mode=True,
    trigger="last",
    leverage=None,
    post_only=False,
    action_price=None,
):
    client = _as_rest(kraken_rest)

    if safe_mode:
        if order_type not in ["market", "stop-loss", "take-profit"]:
            assert (
                price is not None
            ), "Price must be provided for non-market order types."
        if order_type in ["take-profit-limit", "stop-loss-limit"]:
            assert (
                action_price is not None
            ), "Action price must be provided for take-profit and stop-loss order types."
        if order_type != "sell" and type(amount) == str:
            raise ValueError("Amount must be a number for buy orders.")
        if amount == "all":
            warnings.warn(
                "Caution, fetching balances to size an 'all' order is a slow approach "
                "don't use for HFT strategies."
            )
            amount = client.get_balance()[symbol.split("/")[0]]

    payload = {
        "ordertype": order_type,
        "type": side,
        "volume": str(amount),
        "pair": symbol,
        "price": None if price is None else str(price),
        "validate": validate,
        "timeinforce": time_in_force,
        "trigger": trigger,
        "leverage": None if leverage is None else str(leverage),
        "displayvol": None if displayvol is None else str(displayvol),
    }
    if post_only:
        payload["oflags"] = "post"
    if action_price is not None:
        payload["price2"] = str(action_price)

    try:
        result = client.add_order(**payload)
        txid = (result.get("txid") or [None])[0]
        print(f"Order placed successfully: {result}")
        return {
            "id": txid,
            "txid": result.get("txid"),
            "descr": result.get("descr"),
            "info": result,
        }
    except KrakenRESTError as e:
        print(f"Exchange error occurred: {e}")
        return {}
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {}


def cancel_order(kraken_rest, order_id):
    client = _as_rest(kraken_rest)
    try:
        response = client.cancel_order(order_id)
        return response
    except KrakenRESTError as e:
        print(f"Exchange error occurred: {e}")
        return {}
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {}


def cancel_all_by_symbol(kraken_rest, symbol, open_orders=None):
    client = _as_rest(kraken_rest)

    if open_orders is None:
        kp = KrakenPortfolio(client)
        open_orders = kp.open_orders

    if symbol not in open_orders.keys():
        print(f"No open orders for {symbol}")
        return None

    for o in open_orders[symbol]:
        cancel_order(client, o["id"])


def cancel_all(kraken_rest, open_orders=None):
    client = _as_rest(kraken_rest)

    if open_orders is None:
        try:
            return client.cancel_all()
        except KrakenRESTError as e:
            print(f"Exchange error occurred: {e}")
            return {}

    for symbol in open_orders.keys():
        for o in open_orders[symbol]:
            cancel_order(client, o["id"])
