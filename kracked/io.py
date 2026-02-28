import warnings
import sqlite3
import os
import json
import queue
import copy

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from typing import List, Any, Union


class KrackedDB:

    def __init__(self,
                 db_name: str = "kracked_outputs.db",
                 overwrite: bool = False,
                 ):
        """
            This class handles I/O with the SQLite database.

            Parameters
            ----------
            db_name: str
                The name of the database to create.
            overwrite: bool
                Whether to overwrite the database if it already exists. Default is False.

        """
        self.db_name = db_name

        if os.path.exists(db_name):
            if overwrite:
                os.remove(db_name)
            else:
                warnings.warn(f"Database {db_name} already exists. Set overwrite=True to overwrite.")


    def connect(self):
        """
        Connect to the database. Set the cur and con attributes of the class instance.
        """
        self.con = sqlite3.connect(self.db_name)
        self.cur = self.con.cursor()

    def safe_disconnect(self):
        """
        Safely disconnect from the database, after committing changes.
        """
        self.con.commit()
        self.con.close()
        self.cur = None
        self.con = None

    def _check_table_exists(self, table_name: str, depth: int=None) -> bool:
        """
        Check if the requested table exists in the connected database.

        Parameters
        ----------
        table_name (str): The name of the table to check.

        Returns
        -------
        bool: True if the table exists, False otherwise.
        """

        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return self.cur.fetchone() is not None

    def create_table(self, table_name: str, depth: Union[None, int] = None) -> None:

        valids = ["L1", "L2", "L3", "OHLC", "trades"]
        if table_name not in valids:
            raise ValueError(f"Invalid table name: {table_name}, select from {valids}")

        if table_name == "L1":
            self.cur.execute("""CREATE TABLE IF NOT EXISTS L1 (
                                timestamp text,
                                symbol text,
                                bid numeric,
                                bid_qty numeric,
                                ask numeric,
                                ask_qty numeric,
                                last numeric,
                                volume numeric,
                                vwap numeric,
                                low numeric,
                                high numeric,
                                change numeric,
                                change_pct numeric
                            )""")

        elif table_name == "L2":
            if depth is None:
                raise NameError("Must provide depth for L2 book in table creation.")
            elif not isinstance(depth, int):

                # Validate this since we use it in the dynamic construction
                # of the SQL table's columns.
                raise ValueError("Depth must be an integer.")

            columns = ["symbol", "timestamp"]
            for i in range(depth):
                columns.extend( [
                    "ask_px_" + str(i),
                    "ask_sz_" + str(i),
                    "bid_px_" + str(i),
                    "bid_sz_" + str(i),
                ])

            self.cur.execute(f"CREATE TABLE IF NOT EXISTS L2 ({', '.join(columns)})")    
        
        elif table_name == "L3":

            self.cur.execute("""CREATE TABLE IF NOT EXISTS L3 (
                                side text,
                                ts_event text,
                                ts_recv text,
                                price numeric,
                                size numeric,
                                action text,
                                order_id numeric, 
                                symbol text 
                            )
                            """)

        elif table_name == "OHLC":

            self.cur.execute("""CREATE TABLE IF NOT EXISTS OHLC (
                                timestamp text,
                                symbol text,
                                open numeric,
                                high numeric,
                                low numeric,
                                close numeric,
                                volume numeric,
                                vwap numeric,
                                trades numeric,
                                tstart text,
                                ttrue text
                            )
                            """)

        elif table_name == "trades":

            self.cur.execute("""CREATE TABLE IF NOT EXISTS trades (
                                ts_event text,
                                symbol text,
                                price numeric,
                                qty numeric,
                                side text,
                                ord_type text,
                                trade_id numeric
            )""")

    def write_L1(self, l1_data: List[Any]) -> None:
        """
        Write L1 data to the database.

        Parameters
        ----------
        l1_data (List[Any]): The L1 data to write.
        """ 

        self.cur.executemany("INSERT INTO L1 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", l1_data)

    def write_L2(self, l2_data: List[Any], depth: int) -> None:

        """
        Write L2 data to the database.

        Parameters
        ----------
        l2_data (List[Any]): The L2 data to write.
        depth (int): The depth of the L2 data.
        """

        # Ensures the raw code we're going to f-string into an SQL query is valid,
        # and not some potentially problematic SQL code.
        if not isinstance(depth, int):
            raise ValueError("Depth must be an integer.")

        # We need 4 SQL place holders for each depth level, and one for the timestamp.
        placeholder = ["?"]*(depth*4+2)

        self.cur.execute(f"INSERT INTO L2 VALUES ({','.join(placeholder)})", l2_data)


    def write_L3(self, l3_data: List[Any]) -> None:

        """
        Write L3 data to the database.

        Parameters
        ----------
        l3_data (List[Any]): The L3 data to write.  

        """
        self.cur.executemany("INSERT INTO L3 VALUES (?, ?, ?, ?, ?, ?, ?, ?)", l3_data)

    def write_trades(self, trade_data: List[Any]) -> None:

        """
        Write trades data to the database.

        Parameters
        ----------
        trade_data (List[Any]): The trades data to write.
 
        """
        self.cur.executemany("INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?)", trade_data)

    def write_ohlc(self, ohlc_data: List[Any], mode: str) -> None:

        """
        Write OHLC data to the database.

        Parameters
        ----------
        ohlc_data (List[Any]): The OHLC data to write.
        mode (str): The mode to write the data in.
        """

        if mode == "snapshot":
            self.cur.executemany("INSERT INTO OHLC VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ohlc_data)
        elif mode == "update":
            self.cur.execute("INSERT INTO OHLC VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ohlc_data)


class KrackedWriter:
    """
    Dedicated I/O writer that consumes structured payloads from a queue and
    dispatches writes to SQL (via KrackedDB), CSV, or Parquet backends.

    This class is designed to run in its own thread so that all disk/database
    I/O is serialized in one place, keeping feed threads focused on WebSocket
    protocol handling.

    Payload format (dict placed on the queue by feeds):
        {"channel": "L1",    "rows": [[ts, sym, bid, ...], ...]}
        {"channel": "L2",    "symbol": str, "depth": int, "line": [sym, ts, ...]}
        {"channel": "L3",    "ticks": [[side, ts_event, ...], ...]}
        {"channel": "OHLC",  "mode": "update"|"snapshot", "rows": [[...], ...]}
        {"channel": "trades","rows": [[ts, sym, price, ...], ...]}
        {"channel": "instruments", "pairs": [...], "assets": [...], "keys": [...], "header_assets": [...]}
        {"channel": "webapp_l2", "books": {symbol: {"bids": ..., "asks": ...}}}
        None  -- sentinel that causes the writer to flush and exit.

    Parameters
    ----------
    output_queue : queue.Queue
        The queue to consume payloads from.
    output_directory : str
        Base directory for all file outputs.
    output_mode : str
        Default output mode for all channels. Can be "sql", "csv", or "parquet".
    db_name : str
        SQLite database file name (within output_directory).
    channel_modes : dict or None
        Optional per-channel output mode overrides, e.g. {"L2": "parquet", "trades": "sql"}.
    convert_to_parquet_every : int
        For L2 parquet mode: number of updates per symbol before converting
        the temporary CSV to a parquet partition.
    """

    def __init__(
        self,
        output_queue,
        output_directory=".",
        output_mode="sql",
        db_name="kracked_outputs.db",
        channel_modes=None,
        convert_to_parquet_every=1000,
    ):
        self.output_queue = output_queue
        self.output_directory = output_directory
        self.output_mode = output_mode
        self.db_name = db_name
        self.channel_modes = channel_modes or {}
        self.convert_to_parquet_every = convert_to_parquet_every

        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)

        self.db = None
        self._initialized_tables = set()
        self._l2_symbol_counts = {}

    def _get_mode(self, channel):
        """Return the output mode for a given channel, falling back to the global default."""
        return self.channel_modes.get(channel, self.output_mode)

    def _ensure_db(self):
        """Lazily create the KrackedDB instance on first SQL usage."""
        if self.db is None:
            self.db = KrackedDB(db_name=f"{self.output_directory}/{self.db_name}")

    def _ensure_table(self, table_name, depth=None):
        """Lazily create a SQL table on first write for that channel."""
        if table_name not in self._initialized_tables:
            self._ensure_db()
            self.db.connect()
            if depth is not None:
                self.db.create_table(table_name, depth)
            else:
                self.db.create_table(table_name)
            self.db.safe_disconnect()
            self._initialized_tables.add(table_name)

    def run(self):
        """
        Main consumer loop. Call this from a dedicated thread.
        Blocks on queue.get() until a None sentinel is received.
        """
        while True:
            try:
                payload = self.output_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            if payload is None:
                break

            self._dispatch(payload)

    def stop(self):
        """Send the sentinel to shut down the writer loop."""
        self.output_queue.put(None)

    def _dispatch(self, payload):
        """Route a payload to the appropriate write method."""
        channel = payload["channel"]

        if channel == "L1":
            self._write_L1(payload)
        elif channel == "L2":
            self._write_L2(payload)
        elif channel == "L3":
            self._write_L3(payload)
        elif channel == "OHLC":
            self._write_OHLC(payload)
        elif channel == "trades":
            self._write_trades(payload)
        elif channel == "instruments":
            self._write_instruments(payload)
        elif channel == "webapp_l2":
            self._write_webapp_l2(payload)

    # ------------------------------------------------------------------
    # L1
    # ------------------------------------------------------------------

    def _write_L1(self, payload):
        mode = self._get_mode("L1")
        rows = payload["rows"]

        if mode == "csv":
            path = f"{self.output_directory}/L1.csv"
            if not os.path.exists(path):
                with open(path, "w") as fil:
                    fil.write(
                        "timestamp,symbol,bid,bid_qty,ask,ask_qty,last,volume,vwap,low,high,change,change_pct\n"
                    )
            with open(path, "a") as fil:
                for row in rows:
                    fil.write(",".join(row) + "\n")

        elif mode == "sql":
            self._ensure_table("L1")
            self._ensure_db()
            self.db.connect()
            self.db.write_L1(rows)
            self.db.safe_disconnect()

        else:
            raise NotImplementedError(f"L1 output mode '{mode}' not implemented, select csv or sql.")

    # ------------------------------------------------------------------
    # L2
    # ------------------------------------------------------------------

    def _write_L2(self, payload):
        mode = self._get_mode("L2")
        symbol = payload["symbol"]
        depth = payload["depth"]
        line = payload["line"]
        ssymbol = symbol.replace("/", "_")

        if mode == "sql":
            sql_line = [symbol] + line
            self._ensure_table("L2", depth=depth)
            self._ensure_db()
            self.db.connect()
            self.db.write_L2(sql_line, depth)
            self.db.safe_disconnect()

        elif mode in ("csv", "parquet"):
            csv_path = f"{self.output_directory}/L2_{ssymbol}_orderbook.csv"
            if not os.path.exists(csv_path):
                labels = ["timestamp"]
                for i in range(depth):
                    labels.extend([
                        "ask_px_" + str(i),
                        "ask_sz_" + str(i),
                        "bid_px_" + str(i),
                        "bid_sz_" + str(i),
                    ])
                with open(csv_path, "w") as fil:
                    fil.write(",".join(labels) + "\n")
                    fil.write(",".join(line) + "\n")
            else:
                with open(csv_path, "a") as fil:
                    fil.write(",".join(line) + "\n")

            if mode == "parquet":
                if symbol not in self._l2_symbol_counts:
                    self._l2_symbol_counts[symbol] = 0
                self._l2_symbol_counts[symbol] += 1

                if self._l2_symbol_counts[symbol] >= self.convert_to_parquet_every:
                    self._l2_symbol_counts[symbol] = 0
                    df = pd.read_csv(csv_path)
                    table = pa.Table.from_pandas(df)
                    pq.write_to_dataset(
                        table,
                        root_path=f"{self.output_directory}/L2_{ssymbol}_orderbook.parquet",
                    )
                    os.remove(csv_path)

        else:
            raise NotImplementedError(f"L2 output mode '{mode}' not implemented, select csv, parquet, or sql.")

    # ------------------------------------------------------------------
    # L3
    # ------------------------------------------------------------------

    def _write_L3(self, payload):
        mode = self._get_mode("L3")
        ticks = payload["ticks"]
        out_file_name = payload.get("out_file_name", "L3_ticks")

        if mode == "parquet":
            columns = ["side", "ts_event", "ts_recv", "price", "size", "action", "order_id", "symbol"]
            df = pd.DataFrame(ticks, columns=columns)
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(
                table,
                root_path=f"{self.output_directory}/{out_file_name}.parquet",
            )

        elif mode == "csv":
            csv_path = f"{self.output_directory}/{out_file_name}.csv"
            if not os.path.exists(csv_path):
                with open(csv_path, "w") as fil:
                    fil.write("side,ts_event,ts_recv,price,size,action,order_id,symbol\n")
            with open(csv_path, "a") as fil:
                for tick in ticks:
                    tick = [str(t) for t in tick]
                    fil.write(",".join(tick) + "\n")

        elif mode == "sql":
            self._ensure_table("L3")
            self._ensure_db()
            self.db.connect()
            self.db.write_L3(ticks)
            self.db.safe_disconnect()

        else:
            raise NotImplementedError(f"L3 output mode '{mode}' not implemented, select csv, parquet, or sql.")

    # ------------------------------------------------------------------
    # OHLC
    # ------------------------------------------------------------------

    def _write_OHLC(self, payload):
        mode = self._get_mode("OHLC")
        ohlc_mode = payload["mode"]
        rows = payload["rows"]

        if mode == "csv":
            csv_path = f"{self.output_directory}/OHLC.csv"
            if not os.path.exists(csv_path):
                with open(csv_path, "w") as fil:
                    fil.write(
                        "timestamp,symbol,open,high,low,close,volume,vwap,trades,tstart,ttrue\n"
                    )
            with open(csv_path, "a") as fil:
                for row in rows:
                    fil.write(",".join(row) + "\n")

        elif mode == "sql":
            self._ensure_table("OHLC")
            self._ensure_db()
            self.db.connect()
            if ohlc_mode == "update":
                self.db.write_ohlc(rows[0], mode=ohlc_mode)
            else:
                self.db.write_ohlc(rows, mode=ohlc_mode)
            self.db.safe_disconnect()

        else:
            raise NotImplementedError(f"OHLC output mode '{mode}' not implemented, select csv or sql.")

    # ------------------------------------------------------------------
    # Trades
    # ------------------------------------------------------------------

    def _write_trades(self, payload):
        mode = self._get_mode("trades")
        rows = payload["rows"]

        if mode == "parquet":
            columns = ["ts_event", "symbol", "price", "qty", "side", "ord_type", "trade_id"]
            df = pd.DataFrame(rows, columns=columns)
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(
                table,
                root_path=f"{self.output_directory}/trades.parquet",
            )

        elif mode == "csv":
            csv_path = f"{self.output_directory}/trades.csv"
            if not os.path.exists(csv_path):
                with open(csv_path, "w") as fil:
                    fil.write("ts_event,symbol,price,qty,side,ord_type,trade_id\n")
            with open(csv_path, "a") as fil:
                for trade in rows:
                    fil.write(",".join([str(x) for x in trade]) + "\n")

        elif mode == "sql":
            self._ensure_table("trades")
            self._ensure_db()
            self.db.connect()
            self.db.write_trades(rows)
            self.db.safe_disconnect()

        else:
            raise NotImplementedError(f"Trades output mode '{mode}' not implemented, select csv, parquet, or sql.")

    # ------------------------------------------------------------------
    # Instruments (one-shot CSV dump)
    # ------------------------------------------------------------------

    def _write_instruments(self, payload):
        pairs = payload["pairs"]
        assets = payload["assets"]
        keys = payload["keys"]
        header_assets = payload["header_assets"]

        with open(f"{self.output_directory}/kraken_pairs.csv", "w") as fil:
            fil.write(",".join(keys) + "\n")
            for line in pairs:
                fil.write(",".join(line) + "\n")

        with open(f"{self.output_directory}/kraken_assets.csv", "w") as fil:
            fil.write(",".join(header_assets) + "\n")
            for line in assets:
                fil.write(",".join(line) + "\n")

    # ------------------------------------------------------------------
    # Webapp L2 live orderbook JSON
    # ------------------------------------------------------------------

    def _write_webapp_l2(self, payload):
        books = payload["books"]
        with open(f"{self.output_directory}/L2_live_orderbooks.json", "w") as fil:
            json.dump(books, fil)
