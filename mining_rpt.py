#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
TAIFEX History Mining Tool
--------------------------
1. Downloads daily reports from Taiwan Futures Exchange (TAIFEX)
2. Processes and stores data in SQLite database
3. Exports data in various formats (CSV, JSON)
4. Syncs with Google Drive for backup

This script is designed to be run daily to collect and process futures and options data
from the Taiwan Futures Exchange. The data is stored in an SQLite database and can be
exported to various formats for analysis.

Usage:
    python mining_rpt.py -d 20230101-20230131 # Process data for January 2023
    python mining_rpt.py -e TX 300 -d 20230101-20230131 # Export TX data with 300-min intervals
    python mining_rpt.py --upload-recover # Force redownload and reupload

Requirement:
    sudo pip3 install --no-cache-dir numpy PyDrive selenium
    sudo apt-get clean && sudo rm -rf /var/lib/apt/lists/*

Author: Optimized version by Luke Tseng with help from Claude 3.7 Sonnet.
"""

import sys
import os
import zipfile
import argparse

# import re
import sqlite3
import logging
import json
import numpy as np
import time
from datetime import datetime, timedelta
from typing import Tuple, List, Dict, Any, Optional
from pathlib import Path
import subprocess

# Import Google Drive utility
from devices.gdrive import gdrive

# Set up module-level constants
DEFAULT_DB_NAME = "FCT_DB.db"
ITEMS = ("fut_rpt", "opt_rpt")
LOGGER = None  # Will be initialized later


class TaifexReportMiner:
    """
    Main class for mining and processing TAIFEX reports

    This class handles downloading, processing, and storing TAIFEX data.
    It supports both futures and options data, and can export the data
    to various formats for analysis.
    """

    def __init__(self, date: str = None, item: str = "fut_rpt", config_path: str = None):
        """
        Initialize the TAIFEX report miner

        Args:
            date (str, optional): Date in format YYYY_MM_DD. Defaults to today.
            item (str, optional): Report type ('fut_rpt' or 'opt_rpt'). Defaults to 'fut_rpt'.
            config_path (str, optional): Path to config file. Defaults to config.json in script dir.
        """
        self.base_path = Path(os.path.dirname(__file__))
        self.db_path = self.base_path / DEFAULT_DB_NAME

        # Load configuration
        self.config = self._load_config(config_path)

        # Set date and item
        today_str = datetime.today().replace(minute=0, hour=0, second=0, microsecond=0).strftime("%Y_%m_%d")
        self.date = date if date else today_str
        self.item = item

        # Set up report info
        self._setup_report_info()

        # Initialize Google Drive client if needed
        self._init_gdrive()

        LOGGER.info(f"Mining initialized: date='{self.date}', item='{self.item}'")

    def _load_config(self, config_path: str = None) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        if config_path is None:
            config_path = self.base_path / "config.json"

        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _setup_report_info(self):
        """Setup report info based on configuration"""
        try:
            self.report_info = self.config[self.item].copy()

            # Set filename based on report type
            if self.item == "fut_rpt":
                self.report_info["filename"] = f"Daily_{self.date}.zip"
            elif self.item == "opt_rpt":
                self.report_info["filename"] = f"OptionsDaily_{self.date}.zip"

            # Set directory path
            self.report_info["rptdirpath"] = str(self.base_path / self.item)

            LOGGER.info(
                f"Report info: {self.report_info['filename']} in {self.report_info['rptdirpath']} "
                f"via URL: {self.report_info['url']}"
            )
        except KeyError:
            LOGGER.error(f"Configuration error: Item '{self.item}' not found in config")
            raise ValueError(f"Item '{self.item}' not found in configuration")

    def _init_gdrive(self):
        """Initialize Google Drive client if not already initialized"""
        if "gdevice" not in globals():
            global gdevice
            gdevice = gdrive()

    def download_report(self, recover: bool = False) -> Path:
        """
        Download TAIFEX report from the official website

        Args:
            recover: Force download even if file exists

        Returns:
            Path to downloaded file
        """
        # Create report directory if not exists
        report_dir = Path(self.report_info["rptdirpath"])
        report_dir.mkdir(exist_ok=True)

        # Destination file path
        dest_path = report_dir / self.report_info["filename"]

        # Skip download if file exists and not in recover mode
        if not recover and dest_path.exists():
            LOGGER.info(f"File already exists: {dest_path}")
            return dest_path

        # Download the file
        url = f"{self.report_info['url']}/{self.report_info['filename']}"
        LOGGER.info(f"Downloading {dest_path} from {url}")

        tmp_dir = dest_path.parent
        tmp_file = tmp_dir / Path(url).name  # like "report.csv"
        try:
            # Use subprocess instead of os.system for better error handling
            result = subprocess.run(
                # ["wget", "-O", str(dest_path), url],
                ["wget", "-N", url],
                cwd=tmp_dir,
                capture_output=True,
                text=True,
                check=True,
            )
            LOGGER.debug(f"wget output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            LOGGER.error(f"Download failed: {e.stderr}")
            if dest_path.exists():
                dest_path.unlink()  # Remove failed download
            raise RuntimeError(f"Failed to download report: {e}")

        # Rename if needed
        if tmp_file != dest_path:
            tmp_file.rename(dest_path)

        # Verify downloaded ZIP file
        self._verify_zip_file(dest_path)
        return dest_path

    def _verify_zip_file(self, file_path: Path) -> bool:
        """
        Verify that a ZIP file is valid

        Args:
            file_path: Path to ZIP file

        Returns:
            True if file is valid, raises exception otherwise
        """
        try:
            with zipfile.ZipFile(file_path, "r") as zip_file:
                zip_file.testzip()
            LOGGER.info(f"Successfully verified ZIP file: {file_path}")
            return True
        except zipfile.BadZipFile:
            LOGGER.warning(f"Invalid ZIP file: {file_path}")
            if file_path.exists():
                file_path.unlink()
            raise ValueError(f"Downloaded file is not a valid ZIP: {file_path}")

    def extract_report(
        self,
        zip_path: Optional[Path] = None,
        extract_dir: Optional[Path] = None,
    ) -> Path:
        """
        Extract report from ZIP file

        Args:
            zip_path: Path to ZIP file
            extract_dir: Directory to extract to (defaults to tmp subdirectory)

        Returns:
            Path to directory containing extracted files
        """
        # Use provided zip_path or construct from report info
        if zip_path is None:
            zip_path = Path(self.report_info["rptdirpath"]) / self.report_info["filename"]

        # Default extract directory is tmp subdirectory of report directory
        if extract_dir is None:
            extract_dir = Path(self.report_info["rptdirpath"]) / "tmp"

        # Create extract directory if not exists
        extract_dir.mkdir(exist_ok=True)

        # Check if zip file exists
        if not zip_path.exists():
            LOGGER.error(f"ZIP file not found: {zip_path}")
            raise FileNotFoundError(f"ZIP file not found: {zip_path}")

        # Extract files
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_file:
                zip_file.extractall(extract_dir)
                extracted_files = zip_file.namelist()

            LOGGER.info(f"Extracted {len(extracted_files)} files from {zip_path} to {extract_dir}")
            return extract_dir
        except Exception as e:
            LOGGER.error(f"Failed to extract ZIP file: {e}")
            raise RuntimeError(f"Failed to extract ZIP file: {e}")

    def extract_all_reports(self) -> Path:
        """
        Extract all ZIP files in the report directory

        Returns:
            Path to the extraction directory
        """
        LOGGER.info(f"Extracting all reports in {self.report_info['rptdirpath']}")

        report_dir = Path(self.report_info["rptdirpath"])
        extract_dir = report_dir / "tmp"
        extract_dir.mkdir(exist_ok=True)

        if not report_dir.is_dir():
            LOGGER.warning(f"Report directory not found: {report_dir}")
            return extract_dir

        # Process each ZIP file in the directory
        zip_count = 0
        for file_path in report_dir.glob("*.zip"):
            try:
                self._verify_zip_file(file_path)
                with zipfile.ZipFile(file_path, "r") as zip_file:
                    for filename in zip_file.namelist():
                        zip_file.extract(filename, extract_dir)
                        LOGGER.debug(f"Extracted {filename} to {extract_dir}")
                zip_count += 1
            except (zipfile.BadZipFile, Exception) as e:
                LOGGER.warning(f"Skipping {file_path}: {e}")
                continue

        LOGGER.info(f"Extracted {zip_count} ZIP files to {extract_dir}")
        return extract_dir

    def upload_to_gdrive(self, recover: bool = False) -> bool:
        """
        Upload report to Google Drive

        Args:
            recover: Force upload even if file already exists in Google Drive

        Returns:
            True if upload was successful
        """
        file_path = Path(self.report_info["rptdirpath"]) / self.report_info["filename"]

        if not file_path.exists():
            LOGGER.warning(f"File not found for upload: {file_path}")
            return False

        try:
            gdevice.UploadFile(str(file_path), self.item, recover=recover)
            LOGGER.info(f"Successfully uploaded {file_path} to Google Drive")
            return True
        except Exception as e:
            LOGGER.error(f"Failed to upload to Google Drive: {e}")
            return False

    def parse_report_to_db(self, symbol: str = "TX") -> bool:
        """
        Parse report data and store in database

        Args:
            symbol: Futures symbol to parse (e.g., 'TX', 'MTX')

        Returns:
            True if parsing was successful
        """
        # Validate symbol
        if self.item == "fut_rpt" and symbol not in self.report_info.get("symbol", ["TX"]):
            LOGGER.warning(f"Symbol '{symbol}' not in configured symbols. Using default 'TX'")
            symbol = "TX"

        # Paths to ZIP and extracted report
        zip_path = Path(self.report_info["rptdirpath"]) / self.report_info["filename"]
        rpt_path = Path(self.report_info["rptdirpath"]) / "tmp" / self.report_info["filename"].replace(".zip", ".rpt")

        # Ensure the report file exists, extract if not
        if not rpt_path.exists() or args.recover:
            if not zip_path.exists():
                LOGGER.info(f"ZIP file not found locally: {zip_path}")
                try:
                    gdevice.GetContentFile(self.report_info["filename"], str(zip_path))
                except Exception as e:
                    LOGGER.error(f"Failed to retrieve file from Google Drive: {e}")
                    return False
            self.extract_report(zip_path)

        # Process the report file
        return self._process_report_data(rpt_path, symbol)

    def _process_report_data(self, rpt_path: Path, symbol: str) -> bool:
        """
        Process the report data and store in database

        Args:
            rpt_path: Path to the report file
            symbol: Symbol to process

        Returns:
            True if processing was successful
        """
        # Extract date from the report file name
        proc_date = datetime.strptime(self.date, "%Y_%m_%d")

        # Extract data using grep for the specified symbol and month
        current_month = proc_date.strftime("%Y%m")
        grep_cmd = f"cat {rpt_path} | grep ,{symbol} | grep -P '{current_month}\\s+'"
        LOGGER.debug(f"Running grep command: {grep_cmd}")

        tick_result = subprocess.run(grep_cmd, shell=True, capture_output=True, text=True).stdout

        # If no data for current month, try next month (for end-of-month reports)
        if not tick_result.strip():
            next_month = (proc_date + timedelta(weeks=4)).strftime("%Y%m")
            grep_cmd = f"cat {rpt_path} | grep ,{symbol} | grep -P '{next_month}\\s+'"
            LOGGER.debug(f"No data for current month, trying next month: {grep_cmd}")
            tick_result = subprocess.run(grep_cmd, shell=True, capture_output=True, text=True).stdout

        # Clean up the data
        tick_result = tick_result.strip().replace(",", " ").replace("*", " ")
        if not tick_result:
            LOGGER.warning(f"No data found for symbol {symbol} in {rpt_path}")
            return False

        # Convert to numpy array for processing
        raw_data = tick_result.split()
        num_ticks = len(tick_result.splitlines())
        if num_ticks == 0:
            LOGGER.warning(f"No ticks found in the data")
            return False

        # Determine the number of columns in each tick
        tick_cols = len(tick_result.splitlines()[0].split())
        LOGGER.info(f"Found {num_ticks} ticks with {tick_cols} columns each")

        # Validate the data shape
        if len(raw_data) / tick_cols != num_ticks:
            LOGGER.error(f"Data shape mismatch: {len(raw_data)} elements, {tick_cols} columns, {num_ticks} rows")
            return False

        # Reshape the data into a 2D array
        tick_array = np.array(raw_data).reshape(num_ticks, -1)

        # Process the ticks into one-minute candles
        candles = self._process_ticks_to_candles(tick_array)
        if not candles:
            LOGGER.warning(f"No candles generated from the tick data")
            return False

        # Store the processed data in the database
        return self._store_candles_in_db(candles, symbol)

    def _process_ticks_to_candles(self, tick_array: np.ndarray) -> List[Tuple]:
        """
        Convert tick data into one-minute OHLCV candles.

        Args:
            tick_array (np.ndarray): Array of tick data.

        Returns:
            List[Tuple]: List of candle data tuples (Date, Time, Open, High, Low, Close, Volume).
        """
        if tick_array.size == 0:
            LOGGER.warning("Tick array is empty. No candles to process.")
            return []

        # Determine the starting time based on the first tick
        start_time = self._get_start_time(tick_array[0])

        candles = []
        temp_ticks = []
        total_ticks = len(tick_array)

        # Progress bar setup
        progress_step = max(1, total_ticks // 32)

        for i, tick in enumerate(tick_array, 1):
            tick_time = self._parse_tick_time(tick)

            # Check if the tick belongs to the current minute or special times
            if self._is_tick_in_current_minute(tick_time, start_time):
                temp_ticks.append(tuple(tick))
                if i < total_ticks:
                    continue

            # Process the accumulated ticks into a candle
            if temp_ticks:
                candle = self._generate_candle(temp_ticks, start_time)
                candles.append(candle)
                temp_ticks = []

            # Adjust start_time to the next minute
            start_time = self._adjust_start_time(tick, start_time)

            # Update progress bar
            self._update_progress_bar(i, total_ticks, progress_step)

        LOGGER.info(f"Generated {len(candles)} candles from {total_ticks} ticks.")
        return candles

    def _get_start_time(self, first_tick: np.ndarray) -> datetime:
        """
        Determine the starting time based on the first tick.

        Args:
            first_tick (np.ndarray): The first tick in the array.

        Returns:
            datetime: The starting time for candle generation.
        """
        first_tick_time = datetime.strptime(first_tick[3], "%H%M%S")
        if first_tick_time.hour == 15:
            # Night session starts at 15:00
            return datetime.strptime(f"{first_tick[0]}150000", "%Y%m%d%H%M%S") + timedelta(minutes=1)
        else:
            # Day session starts at 08:45
            return datetime.strptime(f"{first_tick[0]}084500", "%Y%m%d%H%M%S") + timedelta(minutes=1)

    def _parse_tick_time(self, tick: np.ndarray) -> datetime:
        """
        Parse the tick time from the tick data.

        Args:
            tick (np.ndarray): A single tick.

        Returns:
            datetime: The parsed tick time.
        """
        return datetime.strptime(f"{tick[0]}{tick[3]}", "%Y%m%d%H%M%S")

    def _is_tick_in_current_minute(self, tick_time: datetime, start_time: datetime) -> bool:
        """
        Check if the tick belongs to the current minute or special times.

        Args:
            tick_time (datetime): The time of the tick.
            start_time (datetime): The current candle's start time.

        Returns:
            bool: True if the tick belongs to the current minute, False otherwise.
        """
        return start_time - timedelta(minutes=1) <= tick_time < start_time or tick_time.time() in [
            datetime.strptime("05:00:00", "%H:%M:%S").time(),
            datetime.strptime("13:45:00", "%H:%M:%S").time(),
        ]

    def _generate_candle(self, ticks: List[Tuple], start_time: datetime) -> Tuple:
        """
        Generate a single OHLCV candle from tick data.

        Args:
            ticks (List[Tuple]): List of ticks for the current minute.
            start_time (datetime): The start time of the candle.

        Returns:
            Tuple: A tuple representing the candle (Date, Time, Open, High, Low, Close, Volume).
        """
        temp_array = np.array(ticks)
        date_str = start_time.strftime("%Y/%m/%d")
        time_str = start_time.strftime("%H:%M:%S")
        open_price = int(temp_array[0, 4])
        high_price = int(temp_array[:, 4].astype("int").max())
        low_price = int(temp_array[:, 4].astype("int").min())
        close_price = int(temp_array[-1, 4])
        volume = int(temp_array[:, 5].astype("int").sum() // 2)  # Divide by 2 for buy/sell records

        LOGGER.debug(
            f"Generated candle: {date_str}, {time_str}, {open_price}, {high_price}, {low_price}, {close_price}, {volume}"
        )
        return (
            date_str,
            time_str,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
        )

    def _adjust_start_time(self, tick: np.ndarray, start_time: datetime) -> datetime:
        """
        Adjust the start time to the next minute.

        Args:
            tick (np.ndarray): The current tick.
            start_time (datetime): The current start time.

        Returns:
            datetime: The adjusted start time.
        """
        return datetime.strptime(f"{tick[0]}{tick[3][:4]}00", "%Y%m%d%H%M%S") + timedelta(minutes=1)

    def _update_progress_bar(self, current_tick: int, total_ticks: int, progress_step: int):
        """
        Update the progress bar in the console.

        Args:
            current_tick (int): The current tick index.
            total_ticks (int): The total number of ticks.
            progress_step (int): The step size for updating the progress bar.
        """
        progress = (current_tick / total_ticks) * 100
        progress_bar = "=" * (current_tick // progress_step) + ">" + " " * (32 - (current_tick // progress_step))
        sys.stdout.write(f"\r[{progress_bar}][{progress:.1f}%]")
        sys.stdout.flush()

    def _store_candles_in_db(self, candles: List[Tuple], symbol: str) -> bool:
        """
        Store candle data in the database

        Args:
            candles: List of candle data tuples
            symbol: Symbol to store data for

        Returns:
            True if storage was successful
        """
        if not candles:
            LOGGER.warning("No candles to store in database")
            return False

        # Connect to database
        db_path = self.base_path / DEFAULT_DB_NAME
        LOGGER.debug(f"Connecting to database: {db_path}")

        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()

            # Use TX as default symbol if not in configured symbols
            if symbol not in self.report_info.get("symbol", ["TX"]):
                LOGGER.warning(f"Symbol '{symbol}' not in configuration, using 'TX'")
                symbol = "TX"

            # Delete existing data for the same date
            # This ensures we don't have duplicate data
            delete_query = f"DELETE FROM tw{symbol} WHERE Date=? AND Time<=?;"
            cursor.execute(delete_query, (candles[-1][0], candles[-1][1]))

            # Special handling for session transitions
            if candles[0][1] == "15:01:00":
                # Handle night session data
                delete_query1 = f"DELETE FROM tw{symbol} WHERE Date=? AND Time>=?;"
                delete_query2 = f"DELETE FROM tw{symbol} WHERE Date=? AND Time<=?;"
                cursor.execute(delete_query1, (candles[0][0], candles[0][1]))
                if len(candles) > 839:  # Specific index from original code
                    cursor.execute(delete_query2, (candles[839][0], candles[839][1]))

            # Commit the deletes
            conn.commit()

            # Insert new data
            insert_query = f"INSERT INTO tw{symbol} VALUES (?,?,?,?,?,?,?);"
            for candle in candles:
                LOGGER.debug(f"Inserting candle: {candle}")
                cursor.execute(insert_query, candle)

            # Commit the inserts
            conn.commit()
            LOGGER.info(f"Successfully stored {len(candles)} candles in database for [{symbol}]")

        except sqlite3.Error as e:
            LOGGER.error(f"Database error: {e}")
            return False
        finally:
            if "conn" in locals():
                conn.close()

    def export_data_to_txt(
        self,
        symbol: str = None,
        interval: int = None,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> str:
        """
        Export data from database to text file

        Args:
            symbol: Symbol to export (e.g., 'TX', 'MTX')
            interval: Time interval in minutes (1, 5, 15, 30, 60, 300)
            start_date: Start date
            end_date: End date

        Returns:
            Path to the exported file
        """
        # Get global args if available
        args = globals().get("args", None)

        # Validate arguments
        if symbol is None or interval is None:
            if args is None or not hasattr(args, "export") or args.export is None or len(args.export) != 2:
                LOGGER.error("Invalid export arguments")
                raise ValueError("Export requires symbol and interval")
            symbol = args.export[0]
            interval = int(args.export[1])

        # Validate symbol
        symbol = "TX" if symbol not in self.report_info.get("symbol", ["TX"]) else symbol

        # Validate interval
        valid_intervals = [1, 5, 15, 30, 60, 300]
        interval = 300 if interval not in valid_intervals else interval

        # Use date range from arguments if not provided
        if start_date is None or end_date is None:
            if args is None or not hasattr(args, "date"):
                today = datetime.today().replace(minute=0, hour=0, second=0, microsecond=0)
                start_date = today
                end_date = today
                LOGGER.warning("No date range provided, using today's date")
            else:
                date_range = validate_date_range(args.date)
                start_date = date_range[0]
                end_date = date_range[1]

        LOGGER.info(
            f"Exporting data: symbol={symbol}, interval={interval}, "
            f"date_range={start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )

        # Connect to database
        db_path = self.base_path / DEFAULT_DB_NAME
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Format date string for output file
        date_string = (
            f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
            if start_date != end_date
            else start_date.strftime("%Y%m%d")
        )

        # Output file path
        output_path = f"{symbol}_{date_string}"

        # Write header to file
        header = "Date,Time,Open,High,Low,Close,Volume"
        with open(output_path, "w") as f:
            f.write(f"{header}\n")

        # Process each day
        current_date = start_date
        while current_date <= end_date:
            formatted_date = current_date.strftime("%Y/%m/%d")
            LOGGER.debug(f"Processing date: {formatted_date}")

            # Get data for the day
            day_data = self._get_data_for_day(cursor, symbol, interval, formatted_date)

            # Write data to file if not empty
            if day_data.strip():
                LOGGER.info(f"Writing data for {formatted_date}")
                with open(output_path, "a") as f:
                    f.write(day_data)

            # Move to next day
            current_date += timedelta(days=1)

        conn.close()
        LOGGER.info(f"Data exported to: {output_path}")

        # Generate JSON data for the last 1.5 years
        self._export_json_data(symbol, start_date)

        return output_path

    def _get_data_for_day(self, cursor, symbol: str, interval: int, date: str) -> str:
        """
        Get data for a specific day

        Args:
            cursor: Database cursor
            symbol: Symbol to get data for
            interval: Time interval
            date: Date to get data for

        Returns:
            Formatted data as string
        """
        result = ""

        # SQL query for time range (8:45 AM to 1:45 PM)
        query = f"""
            SELECT * FROM tw{symbol}
            WHERE Date='{date}'
            AND Time>'08:45:00'
            AND Time<='13:45:00'
            ORDER BY Date, Time;
        """
        cursor.execute(query)

        if interval == 1:
            # Return raw 1-minute data
            rows = cursor.fetchall()
            for row in rows:
                result += f"{','.join(str(x) for x in row)}\n"
        else:
            # Aggregate data by interval
            # aggr_rows = []
            while True:
                rows = cursor.fetchmany(interval)
                if not rows:
                    break

                # Convert to numpy array for easier processing
                data_array = np.array(rows)

                # Calculate OHLCV
                date_val = data_array[-1, 0]  # Use last row's date
                time_val = data_array[-1, 1]  # Use last row's time
                open_val = int(data_array[0, 2])  # First row's open
                high_val = int(np.max(data_array[:, 3].astype("int")))  # Max high
                low_val = int(np.min(data_array[:, 4].astype("int")))  # Min low
                close_val = int(data_array[-1, 5])  # Last row's close
                volume_val = int(np.sum(data_array[:, 6].astype("int")))  # Sum of volume

                # Format as CSV row
                result += f"{date_val},{time_val},{open_val},{high_val},{low_val},{close_val},{volume_val}\n"

        LOGGER.debug(f"Data for {date}: {len(result.splitlines())} rows")
        return result

    def _export_json_data(self, symbol: str, start_date: str) -> str:
        """
        Export data to JSON format for charting

        Args:
            symbol: Symbol to export

        Returns:
            Path to JSON file
        """
        LOGGER.info(f"Generating JSON data for symbol: {symbol}")

        # Output file path
        json_path = f"FUT_{symbol}.json"

        # Date range - use 1.5 years back from today if no file exists
        end_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

        # Initialize data list
        data = []

        # Connect to database
        db_path = self.base_path / DEFAULT_DB_NAME
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        try:
            # If JSON file exists, load it and append only the latest data
            if Path(json_path).exists():
                with open(json_path, "r") as f:
                    data = json.load(f)

                # current_date = datetime.strptime('2025/05/06', '%Y/%m/%d')  # Default start date
                # current_date = end_date
                current_date = start_date
                # assert False, (current_date, type(current_date))
                while current_date <= end_date:
                    # Get only the last day's data to append
                    day_data = self._get_data_for_day(cursor, symbol, 300, current_date.strftime("%Y/%m/%d"))

                    # Process and append if not empty and not already in the file
                    if day_data.strip():
                        LOGGER.info(f"Processing JSON data for {current_date.strftime('%Y-%m-%d')} to append")
                        for line in day_data.strip().splitlines():
                            fields = line.split(",")
                            date_str = fields[0]

                            # Convert to timestamp (milliseconds)
                            timestamp = int(
                                time.mktime((datetime.strptime(date_str, "%Y/%m/%d") + timedelta(hours=23)).timetuple())
                                * 1000
                            )

                            # Check if data for this timestamp already exists
                            if not any(str(timestamp) in str(entry) for entry in data):
                                # Add the entry
                                data.append([timestamp] + [int(x) for x in fields[2:]])
                    # Move to next day
                    current_date += timedelta(days=1)
            else:
                # Generate full data set
                start_date = datetime.strptime("2020/01/01", "%Y/%m/%d")  # Default start date
                current_date = start_date
                while current_date <= end_date:
                    day_data = self._get_data_for_day(cursor, symbol, 300, current_date.strftime("%Y/%m/%d"))

                    # Process if not empty
                    if day_data.strip():
                        LOGGER.info(f"Processing JSON data for {current_date.strftime('%Y-%m-%d')}")
                        for line in day_data.strip().splitlines():
                            fields = line.split(",")
                            date_str = fields[0]

                            # Convert to timestamp (milliseconds)
                            timestamp = int(
                                time.mktime((datetime.strptime(date_str, "%Y/%m/%d") + timedelta(hours=23)).timetuple())
                                * 1000
                            )

                            # Add the entry
                            data.append([timestamp] + [int(x) for x in fields[2:]])

                    # Move to next day
                    current_date += timedelta(days=1)

            with open(json_path, "w") as f:
                json.dump(data, f, indent=4)

            LOGGER.info(f"JSON data exported to: {json_path} with {len(data)} entries")
            return json_path

        except Exception as e:
            LOGGER.error(f"Error exporting JSON data: {e}")
            raise
        finally:
            conn.close()


# Utility functions
def setup_logging(level=logging.INFO) -> logging.Logger:
    """
    Setup and configure logging

    Args:
        level: Logging level

    Returns:
        Logger: Configured logger
    """
    global LOGGER
    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(level)

    # Avoid adding handlers multiple times
    if not LOGGER.handlers:
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(level)
        formatter = logging.Formatter("%(asctime)s | %(name)s - %(levelname)s - %(message)s")
        console.setFormatter(formatter)
        LOGGER.addHandler(console)

    return LOGGER


def validate_date_range(date_text: str, today: datetime = None) -> Tuple[datetime, datetime]:
    """
    Validate and parse date range from string format

    Args:
        date_text: Date range in format YYYYMMDD or YYYYMMDD-YYYYMMDD
        today: Current date reference (for testing)

    Returns:
        Tuple of (start_date, end_date) as datetime objects
    """
    if today is None:
        today = datetime.today().replace(minute=0, hour=0, second=0, microsecond=0)

    date_parts = date_text.split("-")

    try:
        start_date = datetime.strptime(date_parts[0], "%Y%m%d")
        # Set end date to today if not provided
        end_date = today
        if len(date_parts) == 2:
            end_date = datetime.strptime(date_parts[1], "%Y%m%d")
    except ValueError:
        LOGGER.error(f"Invalid date format: '{date_text}'. Expected format: YYYYMMDD or YYYYMMDD-YYYYMMDD")
        raise ValueError(f"Invalid date format: '{date_text}'")

    # Validate date range
    if start_date > end_date or start_date > today:
        LOGGER.error(f"Invalid date range: start date {start_date} is after end date {end_date} or today {today}")
        raise ValueError(f"Invalid date range: {start_date} to {end_date}")

    LOGGER.info(f"Date range: {start_date} to {end_date}")
    return start_date, end_date


def parse_arguments():
    """
    Parse command line arguments

    Returns:
        Parsed arguments
    """
    today = datetime.today().replace(minute=0, hour=0, second=0, microsecond=0)

    parser = argparse.ArgumentParser(description="TAIFEX Report Mining and Processing Tool")
    parser.add_argument(
        "-d",
        "--date",
        type=str,
        default=today.strftime("%Y%m%d"),
        help="Date range for download in format YYYYMMDD or YYYYMMDD-YYYYMMDD",
    )
    parser.add_argument(
        "-e",
        "--export",
        nargs="+",
        type=str,
        default=None,
        help="Export data in format: SYMBOL INTERVAL (e.g., TX 300). Use with -d for date range.",
    )
    parser.add_argument(
        "--upload-recover",
        dest="recover",
        default=False,
        action="store_true",
        help="Force redownload and replace existing files in Google Drive",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level",
    )

    return parser.parse_args()


def main():
    """
    Main entry point for TAIFEX mining script

    This function handles command-line arguments, sets up logging,
    and orchestrates the overall process of downloading, processing,
    and exporting TAIFEX data.
    """
    # Parse command-line arguments
    args = parse_arguments()

    # Setup logging
    setup_logging(getattr(logging, args.log_level))

    # Make args accessible globally
    globals()["args"] = args

    # Validate date range
    start_date, end_date = validate_date_range(args.date)
    LOGGER.info(f"Arguments: {args}")

    # Handle export operation if requested
    if args.export is not None:
        miner = TaifexReportMiner()
        try:
            output_path = miner.export_data_to_txt()
            LOGGER.info(f"Export completed successfully to: {output_path}")
        except Exception as e:
            LOGGER.error(f"Export failed: {e}")
        sys.exit(0)

    # Process each date in the range
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y_%m_%d")
        LOGGER.info(f"Processing date: {date_str}")

        # Process each report type
        for item in ITEMS:
            try:
                # Initialize miner for this date and report type
                miner = TaifexReportMiner(date=date_str, item=item)

                # Download the report
                miner.download_report(recover=args.recover)

                # Upload to Google Drive
                miner.upload_to_gdrive(recover=args.recover)

                # For futures reports, process data for each symbol
                if item == "fut_rpt":
                    for symbol in miner.report_info.get("symbol", ["TX"]):
                        try:
                            miner.parse_report_to_db(symbol)
                        except Exception as e:
                            LOGGER.error(f"Failed to process {symbol} data: {e}")
            except Exception as e:
                LOGGER.error(f"Failed to process {item} for {date_str}: {e}")

        # Move to next date
        current_date += timedelta(days=1)

    LOGGER.info("TAIFEX data mining completed successfully")


if __name__ == "__main__":
    main()

