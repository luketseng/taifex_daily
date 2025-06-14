from lib.log_util import LoggerUtil
import sqlite3
from typing import List, Tuple, Any, Optional

class DBUtils:
    def __init__(self, logger=None, db_path: str = None):
        self.logger = logger or LoggerUtil(name=__name__).get_logger()
        self.db_path = db_path

    def db_connect(self):
        """Return a sqlite3 connection and cursor."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        return conn, cursor

    def db_close(self, conn):
        """Close the sqlite3 connection safely."""
        if conn:
            conn.close()

    def db_execute_query(self, query: str, params: Optional[tuple] = None, fetch: str = "all") -> Any:
        """
        Execute a query and return results.
        Args:
            query: SQL query string
            params: Optional tuple of parameters
            fetch: 'all', 'one', or 'none' (for no fetch)
        Returns:
            Query result or None
        """
        conn, cursor = self.db_connect()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            if fetch == "all":
                result = cursor.fetchall()
            elif fetch == "one":
                result = cursor.fetchone()
            else:
                result = None
            conn.commit()
            return result
        except sqlite3.Error as e:
            self.logger.error(f"DB query error: {e}")
            return None
        finally:
            self.db_close(conn)

    def db_store_generic(self, table: str, columns: List[str], values: List[tuple], delete_where: Optional[str] = None, delete_params: Optional[tuple] = None) -> bool:
        """
        Generic insert (and optional delete-before-insert) for any table.
        Args:
            table: Table name
            columns: List of column names
            values: List of tuples (row values)
            delete_where: Optional WHERE clause for delete (without 'WHERE')
            delete_params: Optional parameters for delete
        Returns:
            True if successful
        """
        if not values:
            self.logger.warning(f"No values to insert into {table}")
            return False
        conn, cursor = self.db_connect()
        try:
            if delete_where:
                delete_query = f"DELETE FROM {table} WHERE {delete_where};"
                cursor.execute(delete_query, delete_params or ())
            placeholders = ','.join(['?'] * len(columns))
            insert_query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders});"
            cursor.executemany(insert_query, values)
            conn.commit()
            self.logger.info(f"Inserted {len(values)} rows into {table}")
            return True
        except sqlite3.Error as e:
            self.logger.error(f"DB insert error: {e}")
            return False
        finally:
            self.db_close(conn)

    def db_delete(self, table: str, where: str, params: Optional[tuple] = None) -> bool:
        """
        Delete rows from a table with a WHERE clause.
        Args:
            table: Table name
            where: WHERE clause (without 'WHERE')
            params: Optional parameters
        Returns:
            True if successful
        """
        conn, cursor = self.db_connect()
        try:
            # select rows to be deleted
            select_query = f"SELECT * FROM {table} WHERE {where};"
            cursor.execute(select_query, params or ())
            rows_to_delete = cursor.fetchall()
            # Format where clause with params for logging
            where_str = where
            if params:
                for value in params:
                    # Add quotes for string values
                    if isinstance(value, str):
                        value_str = f"'{value}'"
                    else:
                        value_str = str(value)
                    where_str = where_str.replace('?', value_str, 1)
            if rows_to_delete:
                self.logger.debug(f"Rows to be deleted from {table} where {where_str}:\n{rows_to_delete}")
            else:
                self.logger.info(f"No rows matched for deletion in {table} where {where_str}")
            # execute delete
            delete_query = f"DELETE FROM {table} WHERE {where};"
            cursor.execute(delete_query, params or ())
            conn.commit()
            self.logger.info(f"Deleted rows from {table} where {where_str}")
            return True
        except sqlite3.Error as e:
            self.logger.error(f"DB delete error: {e}")
            return False
        finally:
            self.db_close(conn)

    def db_store_candles(self, candles: List[Tuple], symbol: str, report_info: dict) -> bool:
        """
        Store candle data in the database, handle deletion and insertion.
        """
        if not candles:
            self.logger.warning("No candles to store in database")
            return False
        self.logger.debug(f"Connecting to database: {self.db_path}")
        try:
            conn, cursor = self.db_connect()
            # Use TX as default symbol if not in configured symbols
            if symbol not in report_info.get("symbol", ["TX"]):
                self.logger.warning(f"Symbol '{symbol}' not in configuration, using 'TX'")
                symbol = "TX"
            # Delete existing data for the same date
            delete_query = f"DELETE FROM tw{symbol} WHERE Date=? AND Time<=?;"
            cursor.execute(delete_query, (candles[-1][0], candles[-1][1]))
            # Special handling for session transitions
            if candles[0][1] == "15:01:00":
                delete_query1 = f"DELETE FROM tw{symbol} WHERE Date=? AND Time>=?;"
                delete_query2 = f"DELETE FROM tw{symbol} WHERE Date=? AND Time<=?;"
                cursor.execute(delete_query1, (candles[0][0], candles[0][1]))
                if len(candles) > 839:
                    cursor.execute(delete_query2, (candles[839][0], candles[839][1]))
            conn.commit()
            # Insert new data
            insert_query = f"INSERT INTO tw{symbol} VALUES (?,?,?,?,?,?,?);"
            for candle in candles:
                self.logger.debug(f"Inserting candle: {candle}")
                cursor.execute(insert_query, candle)
            conn.commit()
            self.logger.info(f"Successfully stored {len(candles)} candles in database for [{symbol}]")
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            return False
        finally:
            self.db_close(conn)
        return True

    def db_get_day_data(self, symbol: str, interval: int, date: str) -> str:
        """
        Query and aggregate data for a specific day and interval.
        """
        result = ""
        conn, cursor = self.db_connect()
        try:
            query = f"""
                SELECT * FROM tw{symbol}
                WHERE Date='{date}'
                AND Time>'08:45:00'
                AND Time<='13:45:00'
                ORDER BY Date, Time;
            """
            cursor.execute(query)
            if interval == 1:
                rows = cursor.fetchall()
                for row in rows:
                    result += f"{','.join(str(x) for x in row)}\n"
            else:
                while True:
                    rows = cursor.fetchmany(interval)
                    if not rows:
                        break
                    import numpy as np
                    data_array = np.array(rows)
                    date_val = data_array[-1, 0]
                    time_val = data_array[-1, 1]
                    open_val = int(data_array[0, 2])
                    high_val = int(np.max(data_array[:, 3].astype("int")))
                    low_val = int(np.min(data_array[:, 4].astype("int")))
                    close_val = int(data_array[-1, 5])
                    volume_val = int(np.sum(data_array[:, 6].astype("int")))
                    result += f"{date_val},{time_val},{open_val},{high_val},{low_val},{close_val},{volume_val}\n"
        finally:
            self.db_close(conn)
        return result
