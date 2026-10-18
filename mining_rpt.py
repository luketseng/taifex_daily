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

# === Standard Library ===
import sys
import os
import argparse
import subprocess
import json
import time
from datetime import datetime, timedelta
from typing import Tuple, List, Dict, Any, Optional
from pathlib import Path

# === Third-Party Libraries ===
import numpy as np

# Import Local Module: log_util and Google Drive utility
from lib.log_util import LoggerUtil
from lib.report_util import ReportUtil
from lib.data_processing import DataProcessing
from lib.db_utils import DBUtils
from lib.export_utils import ExportUtils


# Set up module-level constants
DEFAULT_DB_NAME = "FCT_DB.db"
ITEMS = ("fut_rpt", "opt_rpt")
SESSIONS = ("day", "night", "full")


class TaifexReportMiner:
    """
    Main class for mining and processing TAIFEX reports

    This class handles downloading, processing, and storing TAIFEX data.
    It supports both futures and options data, and can export the data
    to various formats for analysis.
    """

    def __init__(
        self,
        date: str = None,
        item: str = "fut_rpt",
        config_path: str = None,
        init_gdrive: bool = True,
    ):
        """
        Initialize the TAIFEX report miner

        Args:
            date (str, optional): Date in format YYYY_MM_DD. Defaults to today.
            item (str, optional): Report type ('fut_rpt' or 'opt_rpt'). Defaults to 'fut_rpt'.
            config_path (str, optional): Path to config file. Defaults to config.json in script dir.
            init_gdrive (bool, optional): Initialize Google Drive client for download/upload flows.
        """
        self.base_path = Path(os.path.dirname(__file__))
        self.db_path = self.base_path / DEFAULT_DB_NAME

        # Load configuration
        self.config = self._load_config(config_path)

        # Set date and item
        today_str = (
            datetime.today()
            .replace(minute=0, hour=0, second=0, microsecond=0)
            .strftime("%Y_%m_%d")
        )
        self.date = date if date else today_str
        self.item = item

        # Set up report info
        self._setup_report_info()

        # Initialize Google Drive client if needed
        if init_gdrive:
            self._init_gdrive()

        # Initialize downloader
        self.downloader = ReportUtil(report_info=self.report_info)

        # Initialize data processor
        self.data_processor = DataProcessing(show_progress=True)

        # Initialize DB utils
        self.db_utils = DBUtils(db_path=str(self.db_path))

        # Initialize Export utils
        self.exporter = ExportUtils()

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
            from devices.gdrive2 import gdrive

            gdevice = gdrive()

    def download_report(self, recover: bool = False) -> Path:
        """
        Download TAIFEX report from the official website
        Args:
            recover: Force download even if file exists
        Returns:
            Path to downloaded file
        """
        return self.downloader.download_report(recover=recover)

    def _validate_date_range(
        self, date_text: str, today: datetime = None
    ) -> Tuple[datetime, datetime]:
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
            LOGGER.error(
                f"Invalid date format: '{date_text}'. Expected format: YYYYMMDD or YYYYMMDD-YYYYMMDD"
            )
            raise ValueError(f"Invalid date format: '{date_text}'")

        # Validate date range
        if start_date > end_date or start_date > today:
            LOGGER.error(
                f"Invalid date range: start date {start_date} is after end date {end_date} or today {today}"
            )
            raise ValueError(f"Invalid date range: {start_date} to {end_date}")

        LOGGER.info(f"Date range: {start_date} to {end_date}")
        return start_date, end_date

    def extract_report(
        self,
        zip_path: Optional[Path] = None,
        extract_dir: Optional[Path] = None,
    ) -> Path:
        """
        Extract report from ZIP file using ReportUtil
        """
        return self.downloader.extract_report(
            zip_path=zip_path, extract_dir=extract_dir
        )

    def extract_all_reports(self) -> Path:
        """
        Extract all ZIP files in the report directory
        Returns:
            Path to the extraction directory
        """
        return self.downloader.extract_all_reports()

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
        if self.item == "fut_rpt" and symbol not in self.report_info.get(
            "symbol", ["TX"]
        ):
            LOGGER.warning(
                f"Symbol '{symbol}' not in configured symbols. Using default 'TX'"
            )
            symbol = "TX"

        # Paths to ZIP and extracted report
        zip_path = Path(self.report_info["rptdirpath"]) / self.report_info["filename"]
        rpt_path = (
            Path(self.report_info["rptdirpath"])
            / "tmp"
            / self.report_info["filename"].replace(".zip", ".rpt")
        )

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
        Process the report data and store in database using DataProcessing
        """
        candles = self.data_processor.process_report_data(rpt_path, symbol, self.date)
        if not candles:
            LOGGER.warning(f"No candles generated from the tick data")
            return False

        # Store the processed data in the database
        return self._store_candles_in_db(candles, symbol)

    def _store_candles_in_db(self, candles: List[Tuple], symbol: str) -> bool:
        """
        Store candle data in the database using DBUtils (generic method)
        """
        # return self.db_utils.db_store_candles(candles, symbol, self.report_info)
        if not candles:
            LOGGER.warning("No candles to store in database")
            return False
        # Use TX as default symbol if not in configured symbols
        if symbol not in self.report_info.get("symbol", ["TX"]):
            LOGGER.warning(f"Symbol '{symbol}' not in configuration, using 'TX'")
            symbol = "TX"
        table = f"tw{symbol}"
        columns = ["Date", "Time", "Open", "High", "Low", "Close", "Volume"]
        # Delete existing data for the same date (and special session handling)
        # Main delete: all <= last candle
        delete_where = "Date=? AND Time<=?"
        delete_params = (candles[-1][0], candles[-1][1])
        self.db_utils.db_delete(table, delete_where, delete_params)
        # Special handling for session transitions
        if candles[0][1] == "15:01:00":
            delete_where1 = "Date=? AND Time>=?"
            delete_params1 = (candles[0][0], candles[0][1])
            self.db_utils.db_delete(table, delete_where1, delete_params1)
            if len(candles) > 839:
                delete_where2 = "Date=? AND Time<=?"
                delete_params2 = (candles[839][0], candles[839][1])
                self.db_utils.db_delete(table, delete_where2, delete_params2)
        # Insert new data
        return self.db_utils.db_store_generic(table, columns, candles)

    def _get_day_rows(self, table: str, date: str) -> List[Tuple]:
        """
        Get regular session rows for a trading day.
        """
        query = f"""
            SELECT * FROM {table}
            WHERE Date=?
            AND Time>'08:45:00'
            AND Time<='13:45:00'
            ORDER BY Date, Time;
        """
        return self.db_utils.db_execute_query(query, (date,), fetch="all") or []

    def _get_previous_night_start_date(self, table: str, date: str) -> Optional[str]:
        """
        Find the latest calendar date before the trading day that has night rows.
        """
        query = f"""
            SELECT MAX(Date)
            FROM {table}
            WHERE Date<?
            AND Time>'15:00:00';
        """
        row = self.db_utils.db_execute_query(query, (date,), fetch="one")
        return row[0] if row and row[0] else None

    def _get_night_rows(self, table: str, date: str) -> List[Tuple]:
        """
        Get night session rows for a trading day.

        Trading days are represented by the regular session date. The night
        session is the latest available 15:00-05:00 session before that date,
        which allows weekends and exchange holidays to be skipped.
        """
        night_start_date = self._get_previous_night_start_date(table, date)
        if not night_start_date:
            LOGGER.debug(f"No night session found before {date}")
            return []

        night_end_date = (
            datetime.strptime(night_start_date, "%Y/%m/%d") + timedelta(days=1)
        ).strftime("%Y/%m/%d")
        query = f"""
            SELECT * FROM {table}
            WHERE (Date=? AND Time>'15:00:00')
            OR (Date=? AND Time<='05:00:00')
            ORDER BY Date, Time;
        """
        return (
            self.db_utils.db_execute_query(
                query, (night_start_date, night_end_date), fetch="all"
            )
            or []
        )

    def _get_rows_for_session(
        self, symbol: str, date: str, session: str
    ) -> List[Tuple]:
        """
        Get database rows for the requested trading session.
        """
        table = f"tw{symbol}"
        day_rows = self._get_day_rows(table, date)
        if session == "day":
            return day_rows

        # A trading day is identified by its regular session date. If there is
        # no regular session, skip the date to avoid duplicate weekend output.
        if not day_rows:
            LOGGER.debug(f"No regular session found for trading day {date}")
            return []

        night_rows = self._get_night_rows(table, date)
        if session == "night":
            return night_rows
        return night_rows + day_rows

    def _format_rows_for_interval(self, rows: List[Tuple], interval: int) -> str:
        """
        Format database rows as CSV text, aggregating when interval > 1.
        """
        result = ""
        if not rows:
            return result
        if interval == 1:
            for row in rows:
                result += f"{','.join(str(x) for x in row)}\n"
        else:
            idx = 0
            while idx < len(rows):
                chunk = rows[idx : idx + interval]
                if not chunk:
                    break
                data_array = np.array(chunk)
                date_val = data_array[-1, 0]
                time_val = data_array[-1, 1]
                open_val = int(data_array[0, 2])
                high_val = int(np.max(data_array[:, 3].astype("int")))
                low_val = int(np.min(data_array[:, 4].astype("int")))
                close_val = int(data_array[-1, 5])
                volume_val = int(np.sum(data_array[:, 6].astype("int")))
                result += f"{date_val},{time_val},{open_val},{high_val},{low_val},{close_val},{volume_val}\n"
                idx += interval
        return result

    def _get_data_for_day(
        self, symbol: str, interval: int, date: str, session: str = "day"
    ) -> str:
        """
        Get data for a specific trading day using DBUtils.
        """
        rows = self._get_rows_for_session(symbol, date, session)
        result = self._format_rows_for_interval(rows, interval)
        LOGGER.debug(
            f"Data for {date} session={session}: {len(result.splitlines())} rows"
        )
        return result

    def export_data_to_txt(
        self,
        symbol: str = None,
        interval: int = None,
        start_date: datetime = None,
        end_date: datetime = None,
        session: str = None,
        export_json: bool = None,
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
            if (
                args is None
                or not hasattr(args, "export")
                or args.export is None
                or len(args.export) != 2
            ):
                LOGGER.error("Invalid export arguments")
                raise ValueError("Export requires symbol and interval")
            symbol = args.export[0]
            interval = int(args.export[1])

        if session is None:
            session = getattr(args, "session", "day") if args is not None else "day"
        session = session if session in SESSIONS else "day"
        if export_json is None:
            export_json = session == "day"

        # Validate symbol
        symbol = (
            "TX" if symbol not in self.report_info.get("symbol", ["TX"]) else symbol
        )

        # Validate interval
        valid_intervals = [1, 5, 15, 30, 60, 300]
        interval = 300 if interval not in valid_intervals else interval

        # Use date range from arguments if not provided
        if start_date is None or end_date is None:
            if args is None or not hasattr(args, "date"):
                today = datetime.today().replace(
                    minute=0, hour=0, second=0, microsecond=0
                )
                start_date = today
                end_date = today
                LOGGER.warning("No date range provided, using today's date")
            else:
                date_range = self._validate_date_range(args.date)
                start_date = date_range[0]
                end_date = date_range[1]
        LOGGER.info(
            f"Exporting data: symbol={symbol}, interval={interval}, session={session}, "
            f"date_range={start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )

        # Generate date string for file name
        date_string = (
            f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
            if start_date != end_date
            else start_date.strftime("%Y%m%d")
        )

        # Keep the default day-session filename compatible with existing jobs.
        output_path = (
            f"{symbol}_{date_string}"
            if session == "day"
            else f"{symbol}_{date_string}_{session}"
        )

        # Write header to file
        header = "Date,Time,Open,High,Low,Close,Volume"
        all_data = []
        current_date = start_date
        while current_date <= end_date:
            formatted_date = current_date.strftime("%Y/%m/%d")
            LOGGER.debug(f"Processing date: {formatted_date}")

            # Get data for the day
            day_data = self._get_data_for_day(
                symbol, interval, formatted_date, session=session
            )

            # Write data to file if not empty
            if day_data.strip():
                LOGGER.info(f"Writing data for {formatted_date}")
                all_data.extend(day_data.strip().splitlines())
            # Move to next day
            current_date += timedelta(days=1)
        self.exporter.export_to_csv(header, all_data, output_path)
        LOGGER.info(f"Data exported to: {output_path}")

        # Generate visualization JSON only for the regular day-session export.
        if export_json:
            self._export_json_data(symbol, start_date)
        else:
            LOGGER.info(f"Skipping JSON export for session={session}")

        return output_path

    def _export_json_data(self, symbol: str, start_date: str) -> str:
        """
        Export data to JSON format for charting using ExportUtils
        """
        LOGGER.info(f"Generating day-session JSON data for symbol: {symbol}")

        # Output file path
        json_path = f"FUT_{symbol}.json"

        # Date range - use 1.5 years back from today if no file exists
        end_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

        # Initialize data list
        data = []
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
                    day_data = self._get_data_for_day(
                        symbol,
                        300,
                        current_date.strftime("%Y/%m/%d"),
                        session="day",
                    )

                    # Process and append if not empty and not already in the file
                    if day_data.strip():
                        LOGGER.info(
                            f"Processing JSON data for {current_date.strftime('%Y-%m-%d')} to append"
                        )
                        for line in day_data.strip().splitlines():
                            fields = line.split(",")
                            date_str = fields[0]

                            # Convert to timestamp (milliseconds)
                            timestamp = int(
                                time.mktime(
                                    (
                                        datetime.strptime(date_str, "%Y/%m/%d")
                                        + timedelta(hours=23)
                                    ).timetuple()
                                )
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
                start_date_dt = datetime.strptime("2020/01/01", "%Y/%m/%d")
                current_date = start_date_dt
                while current_date <= end_date:
                    day_data = self._get_data_for_day(
                        symbol,
                        300,
                        current_date.strftime("%Y/%m/%d"),
                        session="day",
                    )

                    # Process if not empty
                    if day_data.strip():
                        LOGGER.info(
                            f"Processing JSON data for {current_date.strftime('%Y-%m-%d')}"
                        )
                        for line in day_data.strip().splitlines():
                            fields = line.split(",")
                            date_str = fields[0]

                            # Convert to timestamp (milliseconds)
                            timestamp = int(
                                time.mktime(
                                    (
                                        datetime.strptime(date_str, "%Y/%m/%d")
                                        + timedelta(hours=23)
                                    ).timetuple()
                                )
                                * 1000
                            )

                            # Add the entry
                            data.append([timestamp] + [int(x) for x in fields[2:]])
                    current_date += timedelta(days=1)
            data.sort(key=lambda row: row[0])
            self.exporter.export_to_json(data, json_path)
            LOGGER.info(f"JSON data exported to: {json_path} with {len(data)} entries")
            return json_path

        except Exception as e:
            LOGGER.error(f"Error exporting JSON data: {e}")
            raise


# Utility functions
def parse_arguments():
    """
    Parse command line arguments

    Returns:
        Parsed arguments
    """
    today = datetime.today().replace(minute=0, hour=0, second=0, microsecond=0)

    parser = argparse.ArgumentParser(
        description="TAIFEX Report Mining and Processing Tool"
    )
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
        "-s",
        "--session",
        choices=SESSIONS,
        default="day",
        help="Trading session to export: day, night, or full. Default: day.",
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
    global args
    args = parse_arguments()

    # Initialize logger utility (can use a module name for finer control)
    global LOGGER  # Will be initialized later
    log_util = LoggerUtil(name="mining_rpt", level=args.log_level)  # 20 = logging.INFO
    LOGGER = log_util.get_logger()

    # Make args accessible globally
    # globals()["args"] = args

    # Handle export operation if requested
    if args.export is not None:
        miner = TaifexReportMiner(init_gdrive=False)
        try:
            output_path = miner.export_data_to_txt()
            LOGGER.info(f"Export completed successfully to: {output_path}")
        except Exception as e:
            LOGGER.error(f"Export failed: {e}")
        sys.exit(0)

    # Init TaifexReportMiner()
    miner = TaifexReportMiner(date=args.date)

    # Validate date range
    start_date, end_date = miner._validate_date_range(args.date)
    LOGGER.info(f"Arguments: {args}")

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
