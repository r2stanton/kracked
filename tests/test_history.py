import unittest
from unittest.mock import MagicMock, patch

from kracked.feeds import KrakenOHLC
from kracked.history import (
    KrakenHistory,
    _aggregate_trades_to_ohlc,
    format_ohlc_rows,
    format_trade_rows,
)
from kracked.rest import KrakenREST, kraken_signature


def _json_response(result, status=200, error=None):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = {"error": error or [], "result": result}
    resp.text = ""
    return resp


class TestKrakenSignature(unittest.TestCase):
    def test_signature_is_stable(self):
        data = {"nonce": "1616492376594", "ordertype": "limit", "pair": "XBTUSD"}
        # Secret is dummy base64; just assert the helper returns a string.
        sig = kraken_signature(
            "/0/private/AddOrder",
            data,
            "kQH5HW/8p1uGOBjU6K56l4TqYZgH3/SLrvpI1Eynl7uJ6JrUm9SsjNMq4BpDEw9O",
        )
        self.assertIsInstance(sig, str)
        self.assertGreater(len(sig), 20)


class TestRateLimiting(unittest.TestCase):
    def test_public_throttle_sleeps(self):
        rest = KrakenREST(public_min_interval=0.05, max_retries=0)
        rest._session = MagicMock()
        rest._session.get.return_value = _json_response(
            {"DOGE/USD": [[1, "1", "1", "1", "1", "1", "1", 1]], "last": 1}
        )

        sleeps = []

        def fake_sleep(seconds):
            sleeps.append(seconds)

        with patch("kracked.rest.time.sleep", side_effect=fake_sleep):
            rest.get_ohlc("DOGE/USD", interval=1)
            rest.get_ohlc("DOGE/USD", interval=1)

        self.assertTrue(any(s > 0 for s in sleeps), "second public call should wait")

    def test_history_calls_use_slower_interval(self):
        rest = KrakenREST(
            public_min_interval=0.01,
            history_min_interval=0.2,
            max_retries=0,
        )
        rest._session = MagicMock()
        rest._session.get.return_value = _json_response(
            {
                "DOGE/USD": [["0.1", "1", 100.0, "b", "m", "", 1]],
                "last": "100000000000",
            }
        )

        sleeps = []

        def fake_sleep(seconds):
            sleeps.append(seconds)

        with patch("kracked.rest.time.sleep", side_effect=fake_sleep):
            rest.get_trades("DOGE/USD", paginated=True)
            rest.get_trades("DOGE/USD", paginated=True)

        self.assertTrue(max(sleeps) >= 0.19)

    def test_rate_limit_retries_and_bumps_history_interval(self):
        rest = KrakenREST(
            public_min_interval=0.0,
            history_min_interval=1.6,
            max_retries=2,
        )
        rest._session = MagicMock()
        limited = _json_response({}, error=["EAPI:Rate limit exceeded"])
        ok = _json_response(
            {
                "DOGE/USD": [["0.1", "1", 100.0, "b", "m", "", 1]],
                "last": "1",
            }
        )
        rest._session.get.side_effect = [limited, ok]

        sleeps = []
        with patch("kracked.rest.time.sleep", side_effect=lambda s: sleeps.append(s)):
            pair, trades, last = rest.get_trades("DOGE/USD", paginated=True)

        self.assertEqual(len(trades), 1)
        self.assertGreater(rest._history_interval, 1.6)
        self.assertTrue(any(s >= 5.0 for s in sleeps))


class TestOhlcFormatting(unittest.TestCase):
    def test_rest_ohlc_maps_vwap_and_count(self):
        candles = [
            [1_700_000_000, "1", "3", "0.5", "2", "1.5", "10", 4],
        ]
        rows = format_ohlc_rows("DOGE/USD", 5, candles)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "DOGE/USD")
        self.assertEqual(rows[0][2], "1")
        self.assertEqual(rows[0][6], "10")
        self.assertEqual(rows[0][7], "1.5")
        self.assertEqual(rows[0][8], "4")
        self.assertEqual(rows[0][10], "NaN")


class TestTradeFormatting(unittest.TestCase):
    def test_side_and_type_mapping(self):
        trades = [["0.1", "5", 1_700_000_000.0, "b", "l", "", 99]]
        rows = format_trade_rows("DOGE/USD", trades, recv_ts="recv")
        self.assertEqual(rows[0][2], "DOGE/USD")
        self.assertEqual(rows[0][5], "buy")
        self.assertEqual(rows[0][6], "limit")
        self.assertEqual(rows[0][7], "99")


class TestHistoryPagination(unittest.TestCase):
    def test_stops_at_end_and_writes_cursor(self, tmp_path=None):
        rest = KrakenREST(public_min_interval=0.0, history_min_interval=0.0, max_retries=0)
        rest._session = MagicMock()
        rest._session.get.return_value = _json_response(
            {
                "DOGE/USD": [
                    ["0.1", "1", 100.0, "b", "m", "", 1],
                    ["0.2", "1", 200.0, "s", "m", "", 2],
                    ["0.3", "1", 300.0, "b", "m", "", 3],
                ],
                "last": "300000000000",
            }
        )

        import tempfile, os

        with tempfile.TemporaryDirectory() as td:
            hist = KrakenHistory(output_directory=td, output_mode="csv", rest=rest)
            with patch("kracked.rest.time.sleep"):
                rows = hist.pull_trades(
                    "DOGE/USD",
                    start=0,
                    end=250,
                    write=True,
                    resume=False,
                    progress=False,
                )

            self.assertEqual(len(rows), 2)
            self.assertTrue(
                os.path.exists(os.path.join(td, ".kracked_trades_cursor_DOGE_USD.txt"))
            )
            self.assertTrue(os.path.exists(os.path.join(td, "trades.csv")))

    def test_aggregate_trades_to_ohlc(self):
        trades = format_trade_rows(
            "DOGE/USD",
            [
                ["1", "2", 0.0, "b", "m", "", 1],
                ["3", "2", 10.0, "s", "m", "", 2],
                ["2", "1", 70.0, "b", "m", "", 3],
            ],
            recv_ts="recv",
        )
        rows = _aggregate_trades_to_ohlc("DOGE/USD", 1, trades)
        self.assertEqual(len(rows), 2)
        first = rows[0]
        self.assertEqual(float(first[2]), 1.0)
        self.assertEqual(float(first[3]), 3.0)
        self.assertEqual(float(first[4]), 1.0)
        self.assertEqual(float(first[5]), 3.0)
        self.assertEqual(first[8], "2")


class TestOhlcSnapshotAlias(unittest.TestCase):
    def test_ccxt_snapshot_alias_still_works(self):
        feed = KrakenOHLC(["DOGE/USD"], ccxt_snapshot=True)
        self.assertTrue(feed.ccxt_snapshot)
        self.assertTrue(feed.rest_snapshot)

    def test_rest_snapshot_explicit(self):
        feed = KrakenOHLC(["DOGE/USD"], rest_snapshot=True)
        self.assertTrue(feed.rest_snapshot)


if __name__ == "__main__":
    unittest.main()
