import warnings
import sqlite3
import os
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
            print('making OHLC table')
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
    
