#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
TAIFEX Data Parser Tool
-----------------------
This script fetches and processes trading data from Taiwan Futures Exchange (TAIFEX).
It supports:
1. Parsing and storing futures and options data
2. Data fetching from web sources
3. Data exporting to various formats
4. Data analysis and strategy calculation

Author: Optimized from original by Luke Tseng
"""

import sys
import os
import re
import sqlite3
import json
import time
import chardet
import numpy as np
import argparse
from datetime import datetime, timedelta, date
from typing import List, Dict, Tuple, Any, Optional, Union
from pathlib import Path

# Web scraping modules
from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Constants
DB_NAME = "II_DB.db"
MARKET_DATA_DB = "FCT_DB.db"
DEFAULT_START_DATE = "2020/01/01"


class TaifexDataParser:
    """
    Parser for Taiwan Futures Exchange (TAIFEX) data

    This class handles data retrieval, parsing, storage and analysis
    for futures, options, and spot market data from TAIFEX.
    """

    # Symbol and market participant mapping
    SYMBOL_MAP = {
        "臺股期貨": "TX",
        "電子期貨": "TE",
        "金融期貨": "TF",
        "小型臺指期貨": "MTX",
        "臺指選擇權": "TXO",
        "買權": "CALL",
        "賣權": "PUT",
        "外資及陸資(不含外資自營商)": "FOR",
        "外資及陸資": "FOR",
        "外資": "FOR",
        "外資自營商": "FOR_D",
        "投信": "INV",
        "自營商(自行買賣)": "DEA",
        "自營商(避險)": "DEA_H",
        "自營商": "DEA",
    }

    def __init__(self):
        """Initialize the parser with default values"""
        self.lines_data = []
        self.date = date.today().strftime("%Y/%m/%d")
        self.item = None
        self.base_path = Path(os.path.dirname(__file__))

    def get_db_connection(self, db_name: str = DB_NAME) -> sqlite3.Connection:
        """
        Create a database connection to the specified SQLite database

        Args:
            db_name: Name of the database file

        Returns:
            SQLite connection object
        """
        db_path = self.base_path / db_name
        return sqlite3.connect(db_path)

    def import_data_from_csv(self, item: str) -> None:
        """
        Import data from a CSV file into the database

        Args:
            item: Data type ('Fut' or 'OP')
        """
        # Open CSV file with detected encoding
        csv_path = f"{item}.csv"

        try:
            # Detect encoding
            with open(csv_path, "rb") as f:
                raw_data = f.read(10000)  # Read a sample to detect encoding
                encoding = chardet.detect(raw_data)["encoding"]

            # Read file with correct encoding
            with open(csv_path, "r", encoding=encoding) as f:
                content = f.read()

            # Replace Chinese characters with English symbols
            for chinese, english in self.SYMBOL_MAP.items():
                content = content.replace(chinese, english)

            # Connect to database
            with self.get_db_connection() as conn:
                cursor = conn.cursor()

                # Process and insert data
                lines_to_insert = []
                for line in content.split("\n"):
                    if not line.strip():
                        continue

                    fields = line.split(",")
                    if len(fields) < 12:
                        continue

                    # Check if second field is a recognized symbol
                    if fields[1] in self.SYMBOL_MAP.values():
                        title = repr(fields[:-12]).replace(" ", "").replace("[", "").replace("]", "")
                        values = ",".join(fields[-12:])
                        lines_to_insert.append(f"({title},{values})")

                if lines_to_insert:
                    sql = f"INSERT INTO II_{item} VALUES {','.join(lines_to_insert)};"
                    cursor.execute(sql)
                    conn.commit()
                    print(f"Successfully imported {len(lines_to_insert)} rows of {item} data")
                else:
                    print(f"No valid data found in {csv_path}")

        except Exception as e:
            print(f"Error importing data from CSV: {e}")

    def fetch_data_from_web(self, item: str, target_date: str = None) -> None:
        """
        Fetch data from TAIFEX website

        Args:
            item: Data type ('Fut', 'OP', or 'SPOT')
            target_date: Date to fetch in format 'YYYY/MM/DD', defaults to today
        """
        self.item = item
        if target_date:
            self.date = target_date

        print(f"Fetching {item} data for {self.date}")

        # Setup Chrome options
        options = ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")

        try:
            with Chrome(options=options) as driver:
                self.lines_data = []

                if item == "SPOT":
                    self._fetch_spot_data(driver)
                elif item in ("Fut", "OP"):
                    self._fetch_futures_options_data(driver, item)
                else:
                    print(f"Unknown item type: {item}")
                    return

                # Process and store data
                if self.lines_data:
                    insert_data = self._prepare_data_for_db(item)
                    if insert_data:
                        self._store_data_in_db(*insert_data)
                        print(f"Successfully stored {item} data for {self.date}")
                else:
                    print(f"No data retrieved for {item} on {self.date}")

        except Exception as e:
            print(f"Error fetching data from web: {e}")

    def _fetch_spot_data(self, driver) -> None:
        """
        Fetch spot market data

        Args:
            driver: Selenium WebDriver instance
        """
        # Format date string for URL
        date_str = self.date.replace("/", "")
        url = f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?type=day&dayDate={date_str}&response=html"

        try:
            driver.get(url)
            # Wait for the table to load
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "tr")))

            # Extract table rows
            rows = driver.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                row_list = row.text.split()
                print(row_list)
                self.lines_data.append(row_list)

        except Exception as e:
            print(f"Error fetching spot data: {e}")

    def _fetch_futures_options_data(self, driver, item: str) -> None:
        """
        Fetch futures or options data

        Args:
            driver: Selenium WebDriver instance
            item: 'Fut' for futures or 'OP' for options
        """
        try:
            # Navigate to the appropriate page
            if item == "Fut":
                url = "https://www.taifex.com.tw/cht/3/futContractsDate"
            else:  # 'OP'
                url = "https://www.taifex.com.tw/cht/3/callsAndPutsDate"

            driver.get(url)

            # Set the query date
            date_field = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "queryDate")))

            current_date = date_field.get_attribute("value")
            print(f"Default date on website: {current_date}")

            # Use provided date or keep current date
            target_date = self.date if self.date else current_date
            date_field.clear()
            date_field.send_keys(target_date)
            print(f"Setting date to: {target_date}")

            # Click the search button
            search_button = driver.find_element(By.ID, "button")
            search_button.click()

            # Wait for results to load
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "printhere")))

            # Parse the data table
            self._parse_table_data(driver, item)

        except Exception as e:
            print(f"Error fetching {item} data: {e}")

    def _parse_table_data(self, driver, item: str) -> None:
        """
        Parse data from the table on the TAIFEX website

        Args:
            driver: Selenium WebDriver instance
            item: 'Fut' for futures or 'OP' for options
        """
        try:
            # Find the date displayed in the results
            try:
                date_element = driver.find_element(By.XPATH, '//*[@id="printhere"]/div[4]/p[1]')
                date_span = date_element.find_element(By.CLASS_NAME, "right")
                date_text = date_span.get_attribute("textContent")[2:]
                print(f"Data date from website: {date_text}")

                # Initialize data list with the date
                self.lines_data = [f"{date_text}"]
            except Exception as e:
                print(f"Could not find date information: {e}")
                print("Exiting as no valid data found")
                sys.exit(1)

            # Find and parse the content table
            content_element = driver.find_element(By.XPATH, '//*[@id="printhere"]/div[4]/div[2]/table/tbody')
            rows = content_element.find_elements(By.TAG_NAME, "tr")

            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                row_data = []

                for cell in cells:
                    # Handle different cell structures for Fut and OP
                    divs = cell.find_elements(By.TAG_NAME, "div")
                    if divs:
                        if item == "Fut":
                            cell_content = divs[0].text
                        else:  # OP
                            cell_content = divs[-1].text
                    else:
                        cell_content = cell.text

                    # Clean up the content
                    cell_content = cell_content.strip().replace("\n", "")
                    row_data.append(cell_content)

                # Add the row data to the results
                self.lines_data.append(row_data)

            # Debug output
            print(json.dumps(self.lines_data, indent=4, ensure_ascii=False))

        except Exception as e:
            print(f"Error parsing table data: {e}")

    def _prepare_data_for_db(self, item: str) -> Tuple[str, str]:
        """
        Prepare the data for database insertion

        Args:
            item: Data type ('Fut', 'OP', or 'SPOT')

        Returns:
            Tuple containing (date, SQL value strings)
        """
        if not self.lines_data:
            print("No data to prepare for database")
            return None

        values_list = []
        date_str = ""

        if item in ("Fut", "OP"):
            # Check if we have the date in the data
            if self.date not in self.lines_data[0]:
                print(f"Warning: Expected date {self.date} not found in data: {self.lines_data[0]}")
                return None

            date_str = self.lines_data[0]

            # Process data rows - different number of rows for futures vs options
            data_range = self.lines_data[1:13] if item == "Fut" else self.lines_data[1:7]

            symbol = ""
            ii_type = ""
            pc_type = ""  # Only used for options

            for row in data_range:
                # Parse based on the number of columns in the row
                if len(row) == 15:  # Futures data format
                    symbol = self.SYMBOL_MAP.get(row[1])
                    if not symbol:
                        print(f"Unsupported symbol: {row[1]}")
                        continue
                    ii_type = self.SYMBOL_MAP[row[2]]
                elif len(row) == 13:  # Partial futures data format
                    ii_type = self.SYMBOL_MAP[row[0]]
                elif len(row) == 16:  # Options data format with symbol and put/call
                    symbol = self.SYMBOL_MAP.get(row[1])
                    if not symbol:
                        print(f"Unsupported symbol: {row[1]}")
                        continue
                    pc_type = self.SYMBOL_MAP.get(row[2])
                    ii_type = self.SYMBOL_MAP[row[3]]
                elif len(row) == 14:  # Partial options data format
                    pc_type = self.SYMBOL_MAP.get(row[0])
                    ii_type = self.SYMBOL_MAP[row[1]]
                else:
                    continue

                # Extract and clean the values
                values = [val.replace(",", "") for val in row[-12:]]
                values_str = ",".join(values)

                # Format the SQL values string based on data type
                if item == "Fut":
                    value_str = f"({repr(date_str)},{repr(symbol)},{repr(ii_type)},{values_str})"
                else:  # OP
                    value_str = f"({repr(date_str)},{repr(symbol)},{repr(pc_type)},{repr(ii_type)},{values_str})"

                values_list.append(value_str)

        elif item == "SPOT":
            # Process spot market data
            for row in self.lines_data[:1] + self.lines_data[2:-1]:
                if len(row) == 2:
                    # Parse the date
                    match = re.match(r"(\d+)年(\d+)月(\d+)日", row[0])
                    if match:
                        year = int(match.group(1)) + 1911  # Convert from ROC to Gregorian
                        month = match.group(2)
                        day = match.group(3)
                        date_str = f"{year}/{month}/{day}"

                        # Verify date matches requested date
                        if self.date not in date_str:
                            print(f"Warning: Expected date {self.date} does not match data date {date_str}")
                            return None
                else:
                    # Process market data row
                    ii_type = self.SYMBOL_MAP.get(row[0], "")
                    if not ii_type:
                        continue

                    values = [val.replace(",", "") for val in row[-3:]]
                    values_str = ",".join(values)
                    value_str = f"({repr(date_str)},{repr(ii_type)},{values_str})"
                    values_list.append(value_str)

        if not values_list:
            return None

        return (date_str, ",".join(values_list))

    def _store_data_in_db(self, date_str: str, values_sql: str) -> None:
        """
        Store the prepared data in the database

        Args:
            date_str: Date of the data
            values_sql: SQL values string for insertion
        """
        db_path = self.base_path / DB_NAME
        table_name = f"II_{self.item}"

        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()

                # Delete existing data for this date first
                delete_sql = f"DELETE FROM {table_name} WHERE Date='{date_str}';"
                cursor.execute(delete_sql)

                # Insert new data
                insert_sql = f"INSERT INTO {table_name} VALUES {values_sql};"
                cursor.execute(insert_sql)
                conn.commit()

                print(f"Successfully stored data for {date_str} in table {table_name}")

        except sqlite3.Error as e:
            print(f"Database error: {e}")
        except Exception as e:
            print(f"Error storing data: {e}")

    def execute_query(self, db_path: str, query: str) -> List[Tuple]:
        """
        Execute a database query and return results

        Args:
            db_path: Path to the database file
            query: SQL query string

        Returns:
            List of result tuples
        """
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                return cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Database query error: {e}")
            return []

    def run_trading_strategy(self, target_date: str = None) -> None:
        """
        Calculate and export trading strategy data

        Args:
            target_date: Date to calculate strategy for (default: self.date)
        """
        date_str = target_date if target_date else self.date
        start_date_str = datetime.strptime(DEFAULT_START_DATE, "%Y/%m/%d").strftime("%Y/%m/%d")

        print(f"Calculating trading strategy from {start_date_str} to {date_str}")

        # Database paths
        market_db_path = self.base_path / MARKET_DATA_DB
        ii_db_path = self.base_path / DB_NAME

        try:
            # Get market price data
            query = f"""
                SELECT Date, Time, Close
                FROM twTX
                WHERE Date>="{start_date_str}" AND (Time="13:30:00" OR Time="13:45:00")
                ORDER BY Date, Time;
            """
            price_data = self.execute_query(str(market_db_path), query)

            # Create a dictionary to hold all strategy data
            data_table = {}
            for date_val, time_val, close_val in price_data:
                data_table[date_val] = [close_val]

            # Get futures position data for foreign investors
            query = f"""
                SELECT Date, OI_Net_Contract
                FROM II_Fut
                WHERE Date>='{start_date_str}' AND Fut='TX' AND Institutional='FOR';
            """
            fut_data = self.execute_query(str(ii_db_path), query)

            # Calculate position changes and profit/loss
            prev_position = 0
            for i, (date_val, position) in enumerate(fut_data):
                if date_val not in data_table:
                    continue

                if i > 0:
                    position_change = position - prev_position
                    contract_value = position_change * data_table[date_val][0] * 200  # Contract size = 200
                    total_value_bn = round(contract_value / 100000000, 2)  # Convert to billions
                else:
                    position_change = None
                    total_value_bn = None

                data_table[date_val].extend([position, position_change, total_value_bn])
                prev_position = position

            # Get spot market data
            query = f"""
                SELECT Date, SUM(TR_Net_Amount)
                FROM II_SPOT
                WHERE Date>='{start_date_str}' AND Institutional LIKE 'FOR%'
                GROUP BY Date;
            """
            spot_data = self.execute_query(str(ii_db_path), query)

            for date_val, amount in spot_data:
                if date_val in data_table:
                    # Convert to billions
                    amount_bn = round(amount / 100000000, 2)
                    data_table[date_val].append(amount_bn)

            # Get options data
            with sqlite3.connect(ii_db_path) as conn:
                cursor = conn.cursor()
                query = f"""
                    SELECT Date, OI_B_Contract, OI_S_Contract, OI_B_Amount, OI_S_Amount
                    FROM II_OP
                    WHERE Institutional='FOR' AND Date>='{start_date_str}';
                """
                cursor.execute(query)

                while True:
                    rows = cursor.fetchmany(2)
                    if not rows:
                        break

                    if len(rows) == 2 and rows[0][0] in data_table:
                        # Calculate net options value (buy value + sell value)
                        net_value = ((rows[0][3] + rows[1][4]) - (rows[0][4] + rows[1][3])) / 100000
                        net_value_round = round(net_value, 2)
                        data_table[rows[0][0]].append(net_value_round)

            # Prepare JSON output
            output_data = []
            for date_key, values in data_table.items():
                if len(values) >= 6:  # Only include complete records
                    # Convert date to timestamp (ms)
                    dt = datetime.strptime(date_key, "%Y/%m/%d") + timedelta(hours=23)
                    timestamp = int(time.mktime(dt.timetuple())) * 1000

                    # Format: [timestamp, position_change, total_value_bn, spot_amount_bn, price]
                    output_data.append([timestamp] + values[3:6] + [values[1]])

            # Write to JSON file
            with open("data.json", "w") as f:
                json.dump(output_data, f, indent=4)

            print(f"Strategy data exported to data.json with {len(output_data)} entries")

            # Generate MTX strategy data
            self._generate_mtx_strategy(ii_db_path, start_date_str)

        except Exception as e:
            print(f"Error generating strategy data: {e}")

    def _generate_mtx_strategy(self, db_path: str, start_date_str: str) -> None:
        """
        Generate and export MTX strategy data

        Args:
            db_path: Path to the database
            start_date_str: Start date for analysis
        """
        # try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            query = f"""
                SELECT Date, OI_Net_Contract
                FROM II_Fut
                WHERE Date>='{start_date_str}' AND Fut='MTX'
                ORDER BY Date ASC;
            """
            cursor.execute(query)

            # Process data in groups of 3 rows
            export_info = []
            row_counter = 1

            while True:
                rows = cursor.fetchmany(3)
                if not rows:
                    break

                # Convert to numpy array for easier processing
                rows_array = np.array(rows)
                contract_data = rows_array[:, 1].astype("int")

                # Check if all rows have the same date
                if len(set(rows_array[:, 0])) == 1:
                    date_val = rows_array[0, 0]
                    sum_val = int(np.sum(contract_data))

                    # Calculate moving average after having at least 4 entries
                    if row_counter > 4 and len(export_info) >= 4:
                        # Get last 4 sum values plus current
                        prev_data = np.array(export_info)
                        last_sums = list(prev_data[:, 4][-4:].astype("int"))
                        avg_val = float(np.mean(last_sums + [sum_val]))
                    else:
                        avg_val = 0

                    # Calculate buy/sell signal after having at least 5 entries
                    if row_counter > 5 and len(export_info) >= 1:
                        prev_data = np.array(export_info)
                        prev_avg = float(prev_data[:, 5][-1])
                        bs_signal = round(sum_val - prev_avg, 1)
                    else:
                        bs_signal = 0

                    # Get day of week (1=Monday, 7=Sunday)
                    day_of_week = datetime.strptime(date_val, "%Y/%m/%d").isoweekday()

                    # Append to results
                    export_info.append(
                        [
                            date_val,  # Date
                            *list(contract_data),  # Individual contract data
                            sum_val,  # Sum of contracts
                            avg_val,  # Moving average
                            bs_signal,  # Buy/sell signal
                            day_of_week,  # Day of week
                        ]
                    )

                    row_counter += 1

            # Convert to JSON format with timestamps
            output_data = []
            for row in export_info:
                dt = datetime.strptime(row[0], "%Y/%m/%d") + timedelta(hours=23)
                timestamp = int(time.mktime(dt.timetuple())) * 1000

                # Format: [timestamp, contract_data1, contract_data2, contract_data3, buy_sell_signal]
                II_contract = list(map(str, row[1:4]))
                extra_data = list(map(float, row[5:6]))
                output_data.append([timestamp] + II_contract + extra_data)

            # assert False, output_data
            # Write to JSON file
            with open("data_MTX.json", "w") as f:
                json.dump(output_data, f, indent=4)

            print(f"MTX strategy data exported to data_MTX.json with {len(output_data)} entries")

        # except Exception as e:
        #    print(f"Error generating MTX strategy data: {e}")


def validate_and_convert_date(date_str: str) -> str:
    """
    Validate and convert date string from YYYYMMDD to YYYY/MM/DD format

    Args:
        date_str: Date string in YYYYMMDD format

    Returns:
        str: Date string in YYYY/MM/DD format

    Raises:
        ValueError: If date format is invalid
    """
    try:
        # Parse the input date string
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        # Convert to required format
        return date_obj.strftime("%Y/%m/%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format. Please use YYYYMMDD format (e.g., 20240328). Error: {str(e)}")


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="TAIFEX Data Parser Tool")
    parser.add_argument("-d", "--date", help="Target date in YYYYMMDD format (default: today)", type=str)
    parser.add_argument("-i", "--item", help="Data type to fetch (Fut, OP, or SPOT)", type=str, required=False)
    return parser.parse_args()


def main():
    """Main function to run the data parser"""
    args = parse_args()

    # Initialize parser
    parser = TaifexDataParser()

    # Set target date if provided
    target_date = None
    try:
        target_date = validate_and_convert_date(args.date) if args.date else date.today().strftime("%Y/%m/%d")
    except ValueError as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

    # Uncomment these sections as needed

    # # Import data from CSV file
    # parser.import_data_from_csv(item='Fut')
    # parser.import_data_from_csv(item='OP')
    # sys.exit(0)

    # # Bulk import from a date range
    # start_date = date(2020, 3, 23)
    # while start_date < date.today():
    #     date_str = start_date.strftime('%Y/%m/%d')
    #     print(f"Processing {date_str}")
    #     parser.fetch_data_from_web(item='SPOT', target_date=date_str)
    #     start_date += timedelta(days=1)
    # sys.exit(0)

    # Daily tasks - fetch current day's data
    # today_str = date.today().strftime('%Y/%m/%d')
    # Use specific date for testing if needed
    # today_str = date(2025, 5, 2).strftime('%Y/%m/%d')

    # Fetch futures data
    parser.fetch_data_from_web(item="Fut", target_date=target_date)

    # Fetch options data
    parser.fetch_data_from_web(item="OP", target_date=target_date)

    # Fetch spot market data
    parser.fetch_data_from_web(item="SPOT", target_date=target_date)

    # Run trading strategy calculations
    parser.run_trading_strategy()

    # Run strategy if needed
    # if args.item in ('Fut', 'OP'):
    #    parser.run_trading_strategy(target_date)

    print("Data processing completed successfully")


if __name__ == "__main__":
    main()
