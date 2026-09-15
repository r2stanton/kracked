from kracked.history import KrakenHistory

# One-shot REST history. This does not start a websocket.
#
# Public OHLC is capped at 720 candles. Use pull_trades / pull_ohlc_from_trades
# for longer lookbacks. Paginated calls are spaced ~1.6s apart (and slow further
# if Kraken rate-limits) so a long backfill does not trip the public limiter.

history = KrakenHistory(
    output_directory="ex_out",
    output_mode="csv",
)

ohlc_rows = history.pull_ohlc(["DOGE/USD"], interval=5)
print(f"Pulled {len(ohlc_rows)} OHLC rows")

# Short window so the example stays cheap. Resume cursors live in ex_out/.
trade_rows = history.pull_trades(
    ["DOGE/USD"],
    start="2024-06-01",
    end="2024-06-01T00:05:00",
    max_pages=3,
)
print(f"Pulled {len(trade_rows)} trade rows")
