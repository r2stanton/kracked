"""One-shot historical pulls via Kraken REST.

Writes into the same OHLC / trades schemas the live WebSocket feeds use.
Paginated trade history is paced with the REST client's history interval so
long backfills stay under Kraken's public rate limits.
"""

import datetime
import os
from typing import Any, Dict, Iterable, List, Optional, Union

from kracked.io import KrackedWriter
from kracked.rest import OHLC_INTERVALS, KrakenREST


def _as_symbol_list(symbols: Union[str, List[str]]) -> List[str]:
    if isinstance(symbols, str):
        return [symbols]
    return list(symbols)


def _parse_timestamp(value: Any) -> float:
    """Return a unix-seconds float from int/float/datetime/ISO/'YYYY-MM-DD'."""
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        ts = value
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        return ts.timestamp()
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        ts = datetime.datetime(value.year, value.month, value.day, tzinfo=datetime.timezone.utc)
        return ts.timestamp()
    if isinstance(value, (int, float)):
        # Nanosecond cursors are 19-digit ints.
        if isinstance(value, int) and value > 1e12:
            return value / 1e9
        return float(value)
    text = str(value).strip()
    if not text:
        raise ValueError("Empty timestamp")
    if text.isdigit():
        ivalue = int(text)
        if len(text) > 12:
            return ivalue / 1e9
        return float(ivalue)
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.timestamp()
    except ValueError as exc:
        raise ValueError(f"Could not parse timestamp {value!r}") from exc


def _to_since_ns(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str) and value.isdigit():
        return value
    ts = _parse_timestamp(value)
    if ts is None:
        return None
    if ts <= 0:
        return "0"
    return str(int(ts * 1e9))


def _iso_utc(unix_seconds: float) -> str:
    return datetime.datetime.fromtimestamp(
        unix_seconds, tz=datetime.timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def format_ohlc_rows(symbol: str, interval: int, candles: List[list]) -> List[List[str]]:
    """Map REST OHLC arrays onto the live-feed 11-column row schema."""
    interval_sec = interval * 60
    rows = []
    for candle in candles:
        tstart = float(candle[0])
        tend = tstart + interval_sec
        rows.append(
            [
                _iso_utc(tend),
                symbol,
                str(candle[1]),
                str(candle[2]),
                str(candle[3]),
                str(candle[4]),
                str(candle[6]),
                str(candle[5]),
                str(candle[7]),
                _iso_utc(tstart),
                "NaN",
            ]
        )
    return rows


def format_trade_rows(
    symbol: str,
    trades: List[list],
    recv_ts: Optional[str] = None,
) -> List[list]:
    """Map REST trades onto the live-feed 8-column row schema."""
    if recv_ts is None:
        recv_ts = _iso_utc(datetime.datetime.now(datetime.timezone.utc).timestamp())
    side_map = {"b": "buy", "s": "sell"}
    type_map = {"m": "market", "l": "limit"}
    rows = []
    for trade in trades:
        price, qty, ts_event = trade[0], trade[1], float(trade[2])
        side = side_map.get(str(trade[3]), str(trade[3]))
        ord_type = type_map.get(str(trade[4]), str(trade[4]))
        trade_id = trade[6] if len(trade) >= 7 else ""
        rows.append(
            [
                _iso_utc(ts_event),
                recv_ts,
                symbol,
                str(price),
                str(qty),
                side,
                ord_type,
                str(trade_id),
            ]
        )
    return rows


def _cursor_path(output_directory: str, symbol: str) -> str:
    safe = symbol.replace("/", "_")
    return os.path.join(output_directory, f".kracked_trades_cursor_{safe}.txt")


def _load_cursor(path: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    with open(path, "r") as fil:
        value = fil.read().strip()
    return value or None


def _save_cursor(path: str, cursor: str) -> None:
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    with open(path, "w") as fil:
        fil.write(str(cursor))


class KrakenHistory:
    """Pull historical OHLC and trades through Kraken REST.

    Parameters
    ----------
    output_directory:
        Where CSV/SQL/parquet files (and trade-resume cursors) are written.
    output_mode:
        ``csv``, ``sql``, or ``parquet`` (parquet is trades-only, matching the
        live writer).
    db_name:
        SQLite file name when ``output_mode='sql'``.
    rest:
        Optional preconfigured :class:`KrakenREST`. Pass one to override the
        default history pacing.
    """

    def __init__(
        self,
        output_directory: str = ".",
        output_mode: str = "csv",
        db_name: str = "kracked_outputs.db",
        rest: Optional[KrakenREST] = None,
    ):
        self.output_directory = output_directory
        self.output_mode = output_mode
        self.db_name = db_name
        self.rest = rest or KrakenREST()
        self._writer = None

    def _get_writer(self) -> KrackedWriter:
        if self._writer is None:
            if not os.path.exists(self.output_directory):
                os.makedirs(self.output_directory)
            self._writer = KrackedWriter(
                output_queue=None,
                output_directory=self.output_directory,
                output_mode=self.output_mode,
                db_name=self.db_name,
            )
        return self._writer

    def _write(self, payload: Dict[str, Any]) -> None:
        self._get_writer().write(payload)

    def pull_ohlc(
        self,
        symbols: Union[str, List[str]],
        interval: int = 5,
        write: bool = True,
    ) -> List[List[str]]:
        """Fetch up to 720 REST candles per symbol (Kraken's OHLC cap).

        Returns the 11-column rows used by the live OHLC feed. Native REST
        includes VWAP and trade count, unlike the old CCXT snapshot.
        """
        if interval not in OHLC_INTERVALS:
            raise ValueError(f"interval must be one of {OHLC_INTERVALS}")

        all_rows = []
        for symbol in _as_symbol_list(symbols):
            _, candles, _ = self.rest.get_ohlc(symbol, interval=interval)
            rows = format_ohlc_rows(symbol, interval, candles)
            all_rows.extend(rows)
            if write and rows:
                self._write({"channel": "OHLC", "mode": "snapshot", "rows": rows})
        return all_rows

    def pull_trades(
        self,
        symbols: Union[str, List[str]],
        start: Optional[Any] = None,
        end: Optional[Any] = None,
        write: bool = True,
        resume: bool = True,
        max_pages: Optional[int] = None,
        count: int = 1000,
        progress: bool = True,
    ) -> List[list]:
        """Paginated time-and-sales pull.

        ``start``/``end`` accept unix seconds, ISO timestamps, or ``YYYY-MM-DD``.
        Omit ``start`` to begin at the most recent 1000 trades unless a resume
        cursor exists. Use ``start=0`` for the beginning of the market.

        Cursors are stored under ``output_directory`` so a killed backfill can
        continue. Each page uses the REST history interval (default 1.6s) with
        extra backoff if Kraken rate-limits.
        """
        end_ts = _parse_timestamp(end) if end is not None else None
        collected = []

        for symbol in _as_symbol_list(symbols):
            collected.extend(
                self._pull_trades_symbol(
                    symbol,
                    start=start,
                    end_ts=end_ts,
                    write=write,
                    resume=resume,
                    max_pages=max_pages,
                    count=count,
                    progress=progress,
                )
            )
        return collected

    def _pull_trades_symbol(
        self,
        symbol: str,
        start: Optional[Any],
        end_ts: Optional[float],
        write: bool,
        resume: bool,
        max_pages: Optional[int],
        count: int,
        progress: bool,
    ) -> List[list]:
        cursor_file = _cursor_path(self.output_directory, symbol)
        since = None
        if resume:
            since = _load_cursor(cursor_file)
        if since is None and start is not None:
            since = _to_since_ns(start)

        rows_out = []
        pages = 0
        last_cursor = since

        while True:
            if max_pages is not None and pages >= max_pages:
                break

            _, trades, last = self.rest.get_trades(
                symbol, since=since, count=count, paginated=True
            )
            pages += 1

            if not trades:
                break

            recv_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
            page_rows = []
            reached_end = False
            for trade, formatted in zip(trades, format_trade_rows(symbol, trades, recv_ts)):
                trade_ts = float(trade[2])
                if end_ts is not None and trade_ts >= end_ts:
                    reached_end = True
                    break
                page_rows.append(formatted)

            if write and page_rows:
                self._write({"channel": "trades", "rows": page_rows})
            rows_out.extend(page_rows)

            if progress:
                last_trade_ts = _parse_timestamp(page_rows[-1][0]) if page_rows else None
                print(
                    f"KrakenHistory: {symbol} page {pages}: "
                    f"{len(page_rows)} trades"
                    + (f" through {_iso_utc(last_trade_ts)}" if last_trade_ts else "")
                    + f" (history interval {self.rest._history_interval:.2f}s)"
                )

            if reached_end:
                break
            if last is None or str(last) == str(last_cursor):
                break

            last_cursor = str(last)
            since = last_cursor
            _save_cursor(cursor_file, last_cursor)

        if last_cursor:
            _save_cursor(cursor_file, last_cursor)

        return rows_out

    def pull_ohlc_from_trades(
        self,
        symbols: Union[str, List[str]],
        interval: int = 5,
        start: Optional[Any] = None,
        end: Optional[Any] = None,
        write: bool = True,
        resume: bool = False,
        max_pages: Optional[int] = None,
        progress: bool = True,
    ) -> List[List[str]]:
        """Build OHLC bars from paginated trades (the only path past 720 REST candles)."""
        if interval not in OHLC_INTERVALS:
            raise ValueError(f"interval must be one of {OHLC_INTERVALS}")

        all_rows = []
        for symbol in _as_symbol_list(symbols):
            trades = self.pull_trades(
                symbol,
                start=start,
                end=end,
                write=False,
                resume=resume,
                max_pages=max_pages,
                progress=progress,
            )
            rows = _aggregate_trades_to_ohlc(symbol, interval, trades)
            all_rows.extend(rows)
            if write and rows:
                self._write({"channel": "OHLC", "mode": "snapshot", "rows": rows})
        return all_rows


def _aggregate_trades_to_ohlc(
    symbol: str, interval: int, trade_rows: Iterable[list]
) -> List[List[str]]:
    interval_sec = interval * 60
    buckets: Dict[int, Dict[str, Any]] = {}

    for row in trade_rows:
        ts = _parse_timestamp(row[0])
        price = float(row[3])
        qty = float(row[4])
        start = int(ts // interval_sec) * interval_sec
        bucket = buckets.get(start)
        if bucket is None:
            buckets[start] = {
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": qty,
                "notional": price * qty,
                "trades": 1,
            }
        else:
            bucket["high"] = max(bucket["high"], price)
            bucket["low"] = min(bucket["low"], price)
            bucket["close"] = price
            bucket["volume"] += qty
            bucket["notional"] += price * qty
            bucket["trades"] += 1

    rows = []
    for start in sorted(buckets):
        bucket = buckets[start]
        volume = bucket["volume"]
        vwap = bucket["notional"] / volume if volume else bucket["close"]
        rows.append(
            [
                _iso_utc(start + interval_sec),
                symbol,
                str(bucket["open"]),
                str(bucket["high"]),
                str(bucket["low"]),
                str(bucket["close"]),
                str(volume),
                str(vwap),
                str(bucket["trades"]),
                _iso_utc(start),
                "NaN",
            ]
        )
    return rows
