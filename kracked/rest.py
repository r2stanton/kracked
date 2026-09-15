"""Native Kraken REST client.

Public market-data calls (OHLC, trades) need no keys. Private trading calls
reuse the same HMAC signing the WebSocket token path already used.

Public endpoints are paced at just under 1 request/second by default. Paginated
history pulls use a larger gap so long backfills stay inside Kraken's limits.
"""

import hashlib
import hmac
import base64
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple, Union

import requests


OHLC_INTERVALS = [1, 5, 15, 30, 60, 240, 1440, 10080, 21600]

# Kraken: stay at 1 public call/sec or less. These defaults sit above that.
DEFAULT_PUBLIC_MIN_INTERVAL = 1.25
DEFAULT_PRIVATE_MIN_INTERVAL = 1.0
DEFAULT_HISTORY_MIN_INTERVAL = 1.6
DEFAULT_MAX_RETRIES = 6
DEFAULT_TIMEOUT = 30
RATE_LIMIT_BACKOFF_START = 5.0
RATE_LIMIT_BACKOFF_CAP = 60.0
HISTORY_INTERVAL_CAP = 5.0


class KrakenRESTError(Exception):
    """Raised when Kraken returns a non-empty ``error`` list or HTTP failure."""

    def __init__(self, errors, http_status=None):
        if isinstance(errors, str):
            errors = [errors]
        self.errors = list(errors)
        self.http_status = http_status
        msg = "; ".join(str(e) for e in self.errors)
        if http_status is not None:
            msg = f"HTTP {http_status}: {msg}"
        super().__init__(msg)

    def is_rate_limit(self) -> bool:
        blob = " ".join(str(e) for e in self.errors).lower()
        return (
            "rate limit" in blob
            or "throttled" in blob
            or "exceeded" in blob
            or self.http_status == 429
        )


def kraken_signature(urlpath: str, data: Dict[str, Any], secret: str) -> str:
    """HMAC-SHA512 signature for a private REST path."""
    postdata = urllib.parse.urlencode(data)
    encoded = (str(data["nonce"]) + postdata).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    return base64.b64encode(mac.digest()).decode()


def _is_rate_limit_error(exc: BaseException) -> bool:
    if isinstance(exc, KrakenRESTError):
        return exc.is_rate_limit()
    text = str(exc).lower()
    return "rate limit" in text or "throttled" in text or "429" in text


def _is_unknown_pair(exc: KrakenRESTError) -> bool:
    blob = " ".join(str(e) for e in exc.errors).lower()
    return "unknown pair" in blob or "invalid arguments" in blob


class KrakenREST:
    """Thin Kraken REST wrapper with conservative pacing.

    Parameters
    ----------
    api_key, api_secret:
        Required only for private endpoints.
    public_min_interval:
        Minimum seconds between ordinary public calls (OHLC snapshot, assets).
        Default 1.25s — a buffer over Kraken's "1 per second or less" guidance.
    history_min_interval:
        Minimum seconds between paginated history calls (trades backfill).
        Default 1.6s. Grows after a rate-limit response, up to 5s.
    private_min_interval:
        Minimum seconds between private (signed) calls.
    max_retries:
        Retries after rate-limit / transient errors, with exponential backoff.
    timeout:
        Per-request HTTP timeout in seconds.
    """

    BASE_URL = "https://api.kraken.com"

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        public_min_interval: float = DEFAULT_PUBLIC_MIN_INTERVAL,
        private_min_interval: float = DEFAULT_PRIVATE_MIN_INTERVAL,
        history_min_interval: float = DEFAULT_HISTORY_MIN_INTERVAL,
        max_retries: int = DEFAULT_MAX_RETRIES,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.public_min_interval = float(public_min_interval)
        self.private_min_interval = float(private_min_interval)
        self.history_min_interval = float(history_min_interval)
        self._history_interval = float(history_min_interval)
        self.max_retries = int(max_retries)
        self.timeout = float(timeout)

        self._session = requests.Session()
        self._last_public = 0.0
        self._last_private = 0.0
        self._nonce = 0
        self._asset_pairs = None
        self._assets = None

    # ------------------------------------------------------------------
    # Pacing
    # ------------------------------------------------------------------

    def _throttle(self, kind: str) -> None:
        if kind == "private":
            interval = self.private_min_interval
            last = self._last_private
        elif kind == "history":
            interval = self._history_interval
            last = self._last_public
        else:
            interval = self.public_min_interval
            last = self._last_public

        wait = interval - (time.monotonic() - last)
        if wait > 0:
            time.sleep(wait)

        now = time.monotonic()
        if kind == "private":
            self._last_private = now
        else:
            self._last_public = now

    def _bump_history_interval(self) -> None:
        self._history_interval = min(
            self._history_interval * 1.5,
            HISTORY_INTERVAL_CAP,
        )

    def _backoff_seconds(self, attempt: int) -> float:
        return min(
            RATE_LIMIT_BACKOFF_CAP,
            RATE_LIMIT_BACKOFF_START * (2 ** attempt),
        )

    def _next_nonce(self) -> int:
        nonce = int(time.time() * 1000)
        if nonce <= self._nonce:
            nonce = self._nonce + 1
        self._nonce = nonce
        return nonce

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        private: bool = False,
        paced: str = "public",
    ) -> Dict[str, Any]:
        url = self.BASE_URL + path
        headers = {}
        body = data or {}

        if private:
            if not self.api_key or not self.api_secret:
                raise KrakenRESTError("API key and secret are required for private endpoints")
            body = dict(body)
            body["nonce"] = self._next_nonce()
            headers["API-Key"] = self.api_key
            headers["API-Sign"] = kraken_signature(path, body, self.api_secret)

        last_exc = None
        for attempt in range(self.max_retries + 1):
            self._throttle(paced)
            try:
                if method == "GET":
                    resp = self._session.get(
                        url, params=params, headers=headers, timeout=self.timeout
                    )
                else:
                    resp = self._session.post(
                        url, data=body, headers=headers, timeout=self.timeout
                    )
            except requests.RequestException as exc:
                last_exc = exc
                if attempt >= self.max_retries:
                    raise KrakenRESTError(f"Network error: {exc}")
                time.sleep(self._backoff_seconds(attempt))
                continue

            if resp.status_code == 429:
                last_exc = KrakenRESTError(resp.text or "rate limit exceeded", http_status=429)
                if paced == "history":
                    self._bump_history_interval()
                if attempt >= self.max_retries:
                    raise last_exc
                time.sleep(self._backoff_seconds(attempt))
                continue

            try:
                payload = resp.json()
            except ValueError:
                last_exc = KrakenRESTError(
                    f"Non-JSON response: {resp.text[:200]}",
                    http_status=resp.status_code,
                )
                if attempt >= self.max_retries:
                    raise last_exc
                time.sleep(self._backoff_seconds(attempt))
                continue

            errors = payload.get("error") or []
            if errors:
                last_exc = KrakenRESTError(errors, http_status=resp.status_code)
                if last_exc.is_rate_limit() or any(
                    "Busy" in str(e) or "Unavailable" in str(e) for e in errors
                ):
                    if paced == "history":
                        self._bump_history_interval()
                    if attempt >= self.max_retries:
                        raise last_exc
                    time.sleep(self._backoff_seconds(attempt))
                    continue
                raise last_exc

            if resp.status_code != 200:
                raise KrakenRESTError(
                    f"Unexpected status: {resp.text[:200]}",
                    http_status=resp.status_code,
                )

            return payload.get("result", payload)

        raise last_exc or KrakenRESTError("Request failed")

    def _public(
        self, path: str, params: Optional[Dict[str, Any]] = None, paced: str = "public"
    ) -> Dict[str, Any]:
        return self._request("GET", path, params=params, paced=paced)

    def _private(self, path: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._request(
            "POST", path, data=data or {}, private=True, paced="private"
        )

    # ------------------------------------------------------------------
    # Public market data
    # ------------------------------------------------------------------

    def get_ohlc(
        self,
        pair: str,
        interval: int = 5,
        since: Optional[int] = None,
    ) -> Tuple[str, List[list], Optional[int]]:
        """Return ``(result_pair, candles, last)``.

        Each candle is
        ``[time, open, high, low, close, vwap, volume, count]``.
        At most 720 committed bars; the last row is the in-progress candle.
        """
        if interval not in OHLC_INTERVALS:
            raise ValueError(f"interval must be one of {OHLC_INTERVALS}")

        params = {"pair": pair, "interval": interval, "assetVersion": 1}
        if since is not None:
            params["since"] = since

        try:
            result = self._public("/0/public/OHLC", params=params)
        except KrakenRESTError as exc:
            if not _is_unknown_pair(exc):
                raise
            params.pop("assetVersion", None)
            result = self._public("/0/public/OHLC", params=params)

        return self._split_pair_result(result)

    def get_trades(
        self,
        pair: str,
        since: Optional[Union[str, int]] = None,
        count: int = 1000,
        paginated: bool = False,
    ) -> Tuple[str, List[list], Optional[str]]:
        """Return ``(result_pair, trades, last_cursor)``.

        Each trade is
        ``[price, volume, time, side, orderType, misc, trade_id]``.
        ``last_cursor`` is the nanosecond ``since`` value for the next page.

        Set ``paginated=True`` for history backfills so the slower history
        interval (and post-limit backoff) is used.
        """
        if count < 1 or count > 1000:
            raise ValueError("count must be between 1 and 1000")

        params = {"pair": pair, "count": count, "assetVersion": 1}
        if since is not None:
            params["since"] = since

        paced = "history" if paginated else "public"
        try:
            result = self._public("/0/public/Trades", params=params, paced=paced)
        except KrakenRESTError as exc:
            if not _is_unknown_pair(exc):
                raise
            params.pop("assetVersion", None)
            result = self._public("/0/public/Trades", params=params, paced=paced)

        return self._split_pair_result(result)

    def get_asset_pairs(self) -> Dict[str, Any]:
        if self._asset_pairs is None:
            try:
                self._asset_pairs = self._public(
                    "/0/public/AssetPairs", params={"assetVersion": 1}
                )
            except KrakenRESTError:
                self._asset_pairs = self._public("/0/public/AssetPairs")
        return self._asset_pairs

    def get_assets(self) -> Dict[str, Any]:
        if self._assets is None:
            try:
                self._assets = self._public(
                    "/0/public/Assets", params={"assetVersion": 1}
                )
            except KrakenRESTError:
                self._assets = self._public("/0/public/Assets")
        return self._assets

    def resolve_pair(self, symbol: str) -> str:
        """Map a WS v2 symbol like ``BTC/USD`` to a REST pair parameter."""
        if not symbol:
            raise ValueError("symbol is required")

        pairs = self.get_asset_pairs()
        if symbol in pairs:
            info = pairs[symbol]
            return info.get("altname", symbol)

        compact = symbol.replace("/", "")
        for key, info in pairs.items():
            if not isinstance(info, dict):
                continue
            if (
                key == symbol
                or info.get("altname") == compact
                or info.get("wsname") == symbol
                or info.get("wsname") == symbol.replace("BTC", "XBT")
                or info.get("altname") == compact.replace("BTC", "XBT")
                or info.get("altname") == compact.replace("DOGE", "XDG")
            ):
                return info.get("altname", key)

        if symbol.startswith("BTC/"):
            return "XBT" + compact[3:]
        if symbol.startswith("DOGE/"):
            return "XDG" + compact[4:]
        return compact

    def display_symbol(self, pair_code: str) -> str:
        """Map a REST pair code back to a WS v2 ``BASE/QUOTE`` symbol."""
        pairs = self.get_asset_pairs()
        if pair_code in pairs:
            return pair_code
        for key, info in pairs.items():
            if not isinstance(info, dict):
                continue
            if info.get("altname") == pair_code or info.get("wsname") == pair_code:
                return key
        return (
            pair_code.replace("XBT", "BTC").replace("XDG", "DOGE")
            if "/" in pair_code
            else pair_code
        )

    def normalize_asset(self, code: str) -> str:
        """Map Kraken internal asset codes (``XXBT``, ``ZUSD``) to display names."""
        if not code:
            return code
        assets = self.get_assets()
        if code in assets:
            alt = assets[code].get("altname", code)
            return {"XBT": "BTC", "XDG": "DOGE"}.get(alt, alt)
        stripped = code
        if len(code) == 4 and code[0] in ("X", "Z"):
            stripped = code[1:]
        return {"XBT": "BTC", "XDG": "DOGE"}.get(stripped, stripped)

    @staticmethod
    def _split_pair_result(result: Dict[str, Any]) -> Tuple[str, list, Optional[Any]]:
        last = result.get("last")
        pair_key = None
        data = None
        for key, value in result.items():
            if key == "last":
                continue
            pair_key = key
            data = value
            break
        if pair_key is None or data is None:
            raise KrakenRESTError("Unexpected market-data payload: missing pair data")
        return pair_key, data, last

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def get_ws_token(self) -> str:
        result = self._private("/0/private/GetWebSocketsToken")
        return result["token"]

    def get_balance(self) -> Dict[str, Any]:
        """Return balances keyed by display asset name (``BTC``, ``USD``, ...)."""
        raw = self._private("/0/private/Balance")
        return {self.normalize_asset(k): v for k, v in raw.items()}

    def get_open_orders(self) -> Dict[str, Any]:
        return self._private("/0/private/OpenOrders")

    def add_order(self, **kwargs) -> Dict[str, Any]:
        data = {k: v for k, v in kwargs.items() if v is not None}
        if "pair" in data:
            data["pair"] = self.resolve_pair(str(data["pair"]))
        if "validate" in data:
            data["validate"] = "true" if data["validate"] else "false"
        if "timeinforce" in data and data["timeinforce"]:
            data["timeinforce"] = str(data["timeinforce"]).upper()
        return self._private("/0/private/AddOrder", data=data)

    def cancel_order(self, txid: str) -> Dict[str, Any]:
        return self._private("/0/private/CancelOrder", data={"txid": txid})

    def cancel_all(self) -> Dict[str, Any]:
        return self._private("/0/private/CancelAll")
