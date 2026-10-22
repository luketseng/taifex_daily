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

# === Standard Library ===
import sys
import os
import shutil
import re
import json
import time
import argparse
from datetime import datetime, timedelta, date
from typing import List, Dict, Tuple, Any, Optional, Union
from pathlib import Path

# === Third-Party Libraries ===
import chardet
import numpy as np

# === Web Scraping Modules (Third-Party: Selenium) ===
from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

# === Import Local Module: log_util ===
from lib.log_util import LoggerUtil
from lib.export_utils import ExportUtils
from lib.db_utils import DBUtils
from lib.foreign_positions import export_foreign_positions

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

    def __init__(self, logger=None):
        """Initialize the parser with default values"""
        self.lines_data = []
        self.date = date.today().strftime("%Y/%m/%d")
        self.item = None
        self.base_path = Path(os.path.dirname(__file__))
        self.logger = logger or LoggerUtil(name=__name__).get_logger()
        self.exporter = ExportUtils()
        self.market_db_path = self.base_path / MARKET_DATA_DB
        self.ii_db_path = self.base_path / DB_NAME

    @property
    def dbu_market(self):
        return DBUtils(db_path=str(self.market_db_path))

    @property
    def dbu_ii(self):
        return DBUtils(db_path=str(self.ii_db_path))

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
            with self.dbu_ii.db_connect() as (conn, cursor):

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
                        title = (
                            repr(fields[:-12])
                            .replace(" ", "")
                            .replace("[", "")
                            .replace("]", "")
                        )
                        values = ",".join(fields[-12:])
                        lines_to_insert.append(f"({title},{values})")

                if lines_to_insert:
                    sql = f"INSERT INTO II_{item} VALUES {','.join(lines_to_insert)};"
                    cursor.execute(sql)
                    conn.commit()
                    self.logger.info(
                        f"Successfully imported {len(lines_to_insert)} rows of {item} data"
                    )
                else:
                    self.logger.info(f"No valid data found in {csv_path}")

        except Exception as e:
            self.logger.error(f"Error importing data from CSV: {e}")

    def find_chromedriver(self):
        path = shutil.which("chromedriver")
        if path:
            return path

        # fallback common path
        fallback_paths = [
            "/usr/local/bin/chromedriver",
            "/usr/bin/chromedriver",
            "/opt/chromedriver/chromedriver",
        ]
        for p in fallback_paths:
            if os.path.isfile(p) and os.access(p, os.X_OK):
                return p

        raise RuntimeError("chromedriver not found in PATH or fallback locations.")

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

        # Automatically find chromedriver in PATH
        chromedriver_path = self.find_chromedriver()

        if not chromedriver_path:
            raise RuntimeError(
                "chromedriver not found in PATH. Please install it or add it to PATH."
            )

        # Setup Chrome options
        options = ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-features=VizDisplayCompositor")

        service = Service(executable_path=chromedriver_path)

        try:
            with Chrome(service=service, options=options) as driver:
                self.lines_data = []

                if item == "SPOT":
                    self._fetch_spot_data(driver)
                elif item in ("Fut", "OP"):
                    self._fetch_futures_options_data(driver, item)
                else:
                    self.logger.warning(f"Unknown item type: {item}")
                    return

                # Process and store data
                if self.lines_data:
                    insert_data = self._prepare_data_for_db(item)
                    if insert_data:
                        self._store_data_in_db(*insert_data)
                        self.logger.info(
                            f"Successfully stored {item} data for {self.date}"
                        )
                else:
                    self.logger.warning(f"No data retrieved for {item} on {self.date}")

        except Exception as e:
            self.logger.error(f"Error fetching data from web: {e}")

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
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "tr"))
            )

            # Extract table rows
            rows = driver.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                row_list = row.text.split()
                self.logger.debug(row_list)
                self.lines_data.append(row_list)

        except Exception as e:
            self.logger.error(f"Error fetching spot data: {e}")

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
            date_field = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "queryDate"))
            )

            current_date = date_field.get_attribute("value")
            self.logger.info(f"Default date on website: {current_date}")

            # Use provided date or keep current date
            target_date = self.date if self.date else current_date
            date_field.clear()
            date_field.send_keys(target_date)
            self.logger.info(f"Setting date to: {target_date}")

            # Click the search button
            search_button = driver.find_element(By.ID, "button")
            search_button.click()

            # Wait for results to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "printhere"))
            )

            # Parse the data table
            self._parse_table_data(driver, item)

        except Exception as e:
            self.logger.error(f"Error fetching {item} data: {e}")

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
                date_element = driver.find_element(
                    By.XPATH, '//*[@id="printhere"]/div[4]/p[1]'
                )
                date_span = date_element.find_element(By.CLASS_NAME, "right")
                date_text = date_span.get_attribute("textContent")[2:]
                self.logger.info(f"Data date from website: {date_text}")

                # Initialize data list with the date
                self.lines_data = [f"{date_text}"]
            except Exception as e:
                self.logger.error(f"Could not find date information: {e}")
                self.logger.error("Exiting as no valid data found")
                sys.exit(1)

            # Find and parse the content table
            content_element = driver.find_element(
                By.XPATH, '//*[@id="printhere"]/div[4]/div[2]/table/tbody'
            )
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
            self.logger.debug(json.dumps(self.lines_data, indent=4, ensure_ascii=False))

        except Exception as e:
            self.logger.error(f"Error parsing table data: {e}")

    def _prepare_data_for_db(self, item: str) -> Tuple[str, str]:
        """
        Prepare the data for database insertion

        Args:
            item: Data type ('Fut', 'OP', or 'SPOT')

        Returns:
            Tuple containing (date, SQL value strings)
        """
        if not self.lines_data:
            self.logger.warning("No data to prepare for database")
            return None

        values_list = []
        date_str = ""

        if item in ("Fut", "OP"):
            # Check if we have the date in the data
            if self.date not in self.lines_data[0]:
                self.logger.warning(
                    f"Expected date {self.date} not found in data: {self.lines_data[0]}"
                )
                return None

            date_str = self.lines_data[0]

            # Process data rows - different number of rows for futures vs options
            data_range = (
                self.lines_data[1:13] if item == "Fut" else self.lines_data[1:7]
            )

            symbol = ""
            ii_type = ""
            pc_type = ""  # Only used for options

            for row in data_range:
                # Parse based on the number of columns in the row
                if len(row) == 15:  # Futures data format
                    symbol = self.SYMBOL_MAP.get(row[1])
                    if not symbol:
                        self.logger.warning(f"Unsupported symbol: {row[1]}")
                        continue
                    ii_type = self.SYMBOL_MAP[row[2]]
                elif len(row) == 13:  # Partial futures data format
                    ii_type = self.SYMBOL_MAP[row[0]]
                elif len(row) == 16:  # Options data format with symbol and put/call
                    symbol = self.SYMBOL_MAP.get(row[1])
                    if not symbol:
                        self.logger.warning(f"Unsupported symbol: {row[1]}")
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
                        year = (
                            int(match.group(1)) + 1911
                        )  # Convert from ROC to Gregorian
                        month = match.group(2)
                        day = match.group(3)
                        date_str = f"{year}/{month}/{day}"

                        # Verify date matches requested date
                        if self.date not in date_str:
                            self.logger.warning(
                                f"Warning: Expected date {self.date} does not match data date {date_str}"
                            )
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
        Store the prepared data in the database using DBUtils
        """
        table_name = f"II_{self.item}"
        # Delete existing data for this date first
        self.dbu_ii.db_delete(table_name, "Date=?", (date_str,))
        # Parse values_sql to list of tuples (assume values_sql is like: (a,b,c),(d,e,f))
        import re

        values = []
        for match in re.finditer(r"\((.*?)\)", values_sql):
            row = match.group(1)
            # Remove quotes and split by comma
            row = [
                eval(x) if x.strip().startswith("'") or x.strip().startswith('"') else x
                for x in row.split(",")
            ]
            values.append(tuple(row))
        # Get column count from first row
        if not values:
            self.logger.warning(f"No values to insert into {table_name}")
            return
        columns = [
            f"col{i+1}" for i in range(len(values[0]))
        ]  # Placeholder, ideally should match actual schema
        # Try to get actual column names from DB
        query = f"PRAGMA table_info({table_name});"
        colinfo = self.dbu_ii.db_execute_query(query, fetch="all")
        if colinfo:
            columns = [col[1] for col in colinfo]
        self.dbu_ii.db_store_generic(table_name, columns, values)
        self.logger.info(
            f"Successfully stored data for {date_str} in table {table_name}"
        )

    def _validate_and_convert_date(self, date_str: str) -> str:
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
            raise ValueError(
                f"Invalid date format. Please use YYYYMMDD format (e.g., 20240328). Error: {str(e)}"
            )

    def run_trading_strategy(self, target_date: str = None) -> None:
        """Rebuild strategy JSON by explicit trading-date and product keys."""
        date_str = target_date if target_date else self.date
        start_date_str = datetime.strptime(DEFAULT_START_DATE, "%Y/%m/%d").strftime("%Y/%m/%d")
        self.logger.info(f"Calculating trading strategy from {start_date_str} to {date_str}")
        try:
            # Keep the final available regular-session close for each date.
            price_data = self.dbu_market.db_execute_query(
                "SELECT Date, Time, Close FROM twTX WHERE Date>=? "
                "AND Time IN ('13:30:00','13:45:00') ORDER BY Date, Time",
                (start_date_str,), fetch="all",
            )
            fut_data = self.dbu_ii.db_execute_query(
                "SELECT Date, OI_Net_Contract FROM II_Fut "
                "WHERE Date>=? AND Fut='TX' AND Institutional='FOR' ORDER BY Date",
                (start_date_str,), fetch="all",
            )
            spot_data = self.dbu_ii.db_execute_query(
                "SELECT Date, SUM(TR_Net_Amount) FROM II_SPOT "
                "WHERE Date>=? AND Institutional LIKE 'FOR%' GROUP BY Date ORDER BY Date",
                (start_date_str,), fetch="all",
            )
            option_data = self.dbu_ii.db_execute_query(
                "SELECT Date, PC, OI_B_Amount, OI_S_Amount FROM II_OP "
                "WHERE Date>=? AND OP='TXO' AND Institutional='FOR' "
                "AND PC IN ('CALL','PUT') ORDER BY Date, PC",
                (start_date_str,), fetch="all",
            )
            if any(rows is None for rows in (price_data, fut_data, spot_data, option_data)):
                raise RuntimeError("Strategy source query failed; existing JSON preserved")
            prices = {day: close for day, _, close in price_data}
            positions = {}
            for day, position in fut_data:
                if day in positions:
                    raise ValueError(f"Duplicate foreign TX position: {day}")
                positions[day] = position
            spot = dict(spot_data)
            options = {}
            for day, pc, buy_amount, sell_amount in option_data:
                by_type = options.setdefault(day, {})
                if pc in by_type:
                    raise ValueError(f"Duplicate foreign TXO position: {day} {pc}")
                by_type[pc] = (buy_amount, sell_amount)

            output_data = []
            prev_position = None
            for day in sorted(positions):
                position = positions[day]
                position_change = None if prev_position is None else position - prev_position
                prev_position = position
                if day not in prices or day not in spot:
                    continue
                if set(options.get(day, {})) != {'CALL', 'PUT'}:
                    self.logger.warning(f"Incomplete TXO position data for {day}; skipping strategy row")
                    continue
                # Contract size = 200; convert TWD to units of 100 million.
                futures_amount = None if position_change is None else round(position_change * prices[day] * 200 / 100000000, 2)
                spot_amount = round(spot[day] / 100000000, 2)
                call_buy, call_sell = options[day]['CALL']
                put_buy, put_sell = options[day]['PUT']
                # Calls and puts contribute opposite directional exposure.
                options_amount = round((call_buy + put_sell - call_sell - put_buy) / 100000, 2)
                dt = datetime.strptime(day, "%Y/%m/%d") + timedelta(hours=23)
                timestamp = int(time.mktime(dt.timetuple())) * 1000
                # Preserve the existing frontend array contract.
                output_data.append([timestamp, futures_amount, spot_amount, options_amount, position])
            if not output_data:
                raise ValueError("No complete strategy rows; existing JSON preserved")
            self.exporter.export_to_json(output_data, "data.json")
            self.logger.info(f"Strategy data exported to data.json with {len(output_data)} entries")
            self._generate_mtx_strategy(self.ii_db_path, start_date_str)
        except Exception as e:
            self.logger.error(f"Error generating strategy data: {e}")
            raise

    def _generate_mtx_strategy(self, db_path: str, start_date_str: str) -> None:
        """Rebuild MTX strategy with fixed dealer, investment trust, foreign columns."""
        rows = DBUtils(db_path=db_path).db_execute_query(
            "SELECT Date, Institutional, OI_Net_Contract FROM II_Fut "
            "WHERE Date>=? AND Fut='MTX' AND Institutional IN ('DEA','INV','FOR') "
            "ORDER BY Date, Institutional",
            (start_date_str,), fetch="all",
        )
        if rows is None:
            raise RuntimeError("MTX source query failed; existing JSON preserved")
        grouped = {}
        for day, institution, position in rows:
            by_institution = grouped.setdefault(day, {})
            if institution in by_institution:
                raise ValueError(f"Duplicate MTX position: {day} {institution}")
            by_institution[institution] = position
        output_data = []
        totals = []
        for day in sorted(grouped):
            positions = grouped[day]
            if set(positions) != {'DEA', 'INV', 'FOR'}:
                self.logger.warning(f"Incomplete MTX institutions for {day}; skipping row")
                continue
            # Individual contract data always follows the frontend series order.
            contracts = [int(positions[key]) for key in ('DEA', 'INV', 'FOR')]
            totals.append(sum(contracts))
            # Preserve the five-day moving average and its four-row warmup.
            average = float(sum(totals[-5:]) / 5) if len(totals) >= 5 else 0.0
            dt = datetime.strptime(day, "%Y/%m/%d") + timedelta(hours=23)
            timestamp = int(time.mktime(dt.timetuple())) * 1000
            output_data.append([timestamp] + list(map(str, contracts)) + [average])
        if not output_data:
            raise ValueError("No complete MTX strategy rows; existing JSON preserved")
        self.exporter.export_to_json(output_data, "data_MTX.json")
        self.logger.info(f"MTX strategy data exported to data_MTX.json with {len(output_data)} entries")


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="TAIFEX Data Parser Tool")
    parser.add_argument(
        "-d", "--date", help="Target date in YYYYMMDD format (default: today)", type=str
    )
    parser.add_argument(
        "-i",
        "--item",
        help="Data type to fetch (Fut, OP, or SPOT)",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level",
    )
    return parser.parse_args()


def main():
    """Main function to run the data parser"""
    args = parse_args()

    # Initialize logger utility (can use a module name for finer control)
    global LOGGER  # Will be initialized later
    log_util = LoggerUtil(name="get_data", level=args.log_level)  # 20 = logging.INFO
    LOGGER = log_util.get_logger()

    parser = TaifexDataParser(logger=LOGGER)

    # Set target date if provided
    target_date = None
    try:
        target_date = (
            parser._validate_and_convert_date(args.date)
            if args.date
            else date.today().strftime("%Y/%m/%d")
        )
    except ValueError as e:
        LOGGER.error(f"Error: {str(e)}")
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

    # Export only the requested day, preserving all existing position history.
    positions_output = Path.cwd() / "foreign_positions.json"
    if export_foreign_positions(parser.ii_db_path, target_date, positions_output):
        LOGGER.info(f"Foreign positions exported for {target_date}: {positions_output}")
    else:
        LOGGER.warning(f"Incomplete foreign positions for {target_date}; existing JSON preserved")

    LOGGER.info("Data processing completed successfully")


if __name__ == "__main__":
    main()
