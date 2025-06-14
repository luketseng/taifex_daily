from lib.log_util import LoggerUtil
import numpy as np
from typing import List, Tuple
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import sys

class DataProcessing:
    def __init__(self, logger=None, show_progress: bool = False):
        self.logger = logger or LoggerUtil(name=__name__).get_logger()
        self.show_progress = show_progress

    def process_report_data(self, rpt_path: Path, symbol: str, date: str) -> List[Tuple]:
        """
        Parse and process raw report file, return list of 1min OHLCV tuples.
        Args:
            rpt_path: Path to the report file
            symbol: Symbol to process
            date: Date string (YYYY_MM_DD)
        Returns:
            List of (Date, Time, Open, High, Low, Close, Volume)
        """
        proc_date = datetime.strptime(date, "%Y_%m_%d")
        current_month = proc_date.strftime("%Y%m")
        grep_cmd = f"cat {rpt_path} | grep ,{symbol} | grep -P '{current_month}\\s+'"
        self.logger.debug(f"Running grep command: {grep_cmd}")
        tick_result = subprocess.run(grep_cmd, shell=True, capture_output=True, text=True).stdout
        if not tick_result.strip():
            next_month = (proc_date + timedelta(weeks=4)).strftime("%Y%m")
            grep_cmd = f"cat {rpt_path} | grep ,{symbol} | grep -P '{next_month}\\s+'"
            self.logger.debug(f"No data for current month, trying next month: {grep_cmd}")
            tick_result = subprocess.run(grep_cmd, shell=True, capture_output=True, text=True).stdout
        tick_result = tick_result.strip().replace(",", " ").replace("*", " ")
        if not tick_result:
            self.logger.warning(f"No data found for symbol {symbol} in {rpt_path}")
            return []
        raw_data = tick_result.split()
        num_ticks = len(tick_result.splitlines())
        if num_ticks == 0:
            self.logger.warning(f"No ticks found in the data")
            return []
        tick_cols = len(tick_result.splitlines()[0].split())
        self.logger.info(f"Found {num_ticks} ticks with {tick_cols} columns each")
        if len(raw_data) / tick_cols != num_ticks:
            self.logger.error(f"Data shape mismatch: {len(raw_data)} elements, {tick_cols} columns, {num_ticks} rows")
            return []
        tick_array = np.array(raw_data).reshape(num_ticks, -1)
        candles = self.process_ticks_to_candles(tick_array)
        if not candles:
            self.logger.warning(f"No candles generated from the tick data")
            return []
        return candles

    def process_ticks_to_candles(self, tick_array: np.ndarray) -> List[Tuple]:
        """
        Convert tick data into one-minute OHLCV candles.
        """
        if tick_array.size == 0:
            self.logger.warning("Tick array is empty. No candles to process.")
            return []
        start_time = self.get_start_time(tick_array[0])
        candles = []
        temp_ticks = []
        total_ticks = len(tick_array)
        progress_step = max(1, total_ticks // 32)
        for i, tick in enumerate(tick_array, 1):
            tick_time = self.parse_tick_time(tick)
            if self.is_tick_in_current_minute(tick_time, start_time):
                temp_ticks.append(tuple(tick))
                if i < total_ticks:
                    continue
            if temp_ticks:
                candle = self.generate_candle(temp_ticks, start_time)
                candles.append(candle)
                temp_ticks = []
            start_time = self.adjust_start_time(tick, start_time)
            if self.show_progress:
                self.update_progress_bar(i, total_ticks, progress_step)
        if self.show_progress:
            sys.stdout.write("\n")  # Add newline after progress bar completes
            sys.stdout.flush()
        self.logger.info(f"Generated {len(candles)} candles from {total_ticks} ticks.")
        return candles

    def get_start_time(self, first_tick: np.ndarray) -> datetime:
        first_tick_time = datetime.strptime(first_tick[3], "%H%M%S")
        if first_tick_time.hour == 15:
            return datetime.strptime(f"{first_tick[0]}150000", "%Y%m%d%H%M%S") + timedelta(minutes=1)
        else:
            return datetime.strptime(f"{first_tick[0]}084500", "%Y%m%d%H%M%S") + timedelta(minutes=1)

    def parse_tick_time(self, tick: np.ndarray) -> datetime:
        return datetime.strptime(f"{tick[0]}{tick[3]}", "%Y%m%d%H%M%S")

    def is_tick_in_current_minute(self, tick_time: datetime, start_time: datetime) -> bool:
        return start_time - timedelta(minutes=1) <= tick_time < start_time or tick_time.time() in [
            datetime.strptime("05:00:00", "%H:%M:%S").time(),
            datetime.strptime("13:45:00", "%H:%M:%S").time(),
        ]

    def generate_candle(self, ticks: List[Tuple], start_time: datetime) -> Tuple:
        temp_array = np.array(ticks)
        date_str = start_time.strftime("%Y/%m/%d")
        time_str = start_time.strftime("%H:%M:%S")
        open_price = int(temp_array[0, 4])
        high_price = int(temp_array[:, 4].astype("int").max())
        low_price = int(temp_array[:, 4].astype("int").min())
        close_price = int(temp_array[-1, 4])
        volume = int(temp_array[:, 5].astype("int").sum() // 2)
        self.logger.debug(
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

    def adjust_start_time(self, tick: np.ndarray, start_time: datetime) -> datetime:
        return datetime.strptime(f"{tick[0]}{tick[3][:4]}00", "%Y%m%d%H%M%S") + timedelta(minutes=1)

    def update_progress_bar(self, current_tick: int, total_ticks: int, progress_step: int):
        progress = (current_tick / total_ticks) * 100
        progress_bar = "=" * (current_tick // progress_step) + ">" + " " * (32 - (current_tick // progress_step))
        sys.stdout.write(f"\r[{progress_bar}][{progress:.1f}%]")
        sys.stdout.flush()