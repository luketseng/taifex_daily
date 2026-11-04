"""Validate the existing positional contracts before publishing dashboard JSON."""
import math
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _number(value, label, integer=False):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f'{label} must be a finite number')
    if integer and int(value) != value:
        raise ValueError(f'{label} must contain whole contracts')


def validate_dashboard_json(data, output_path):
    """Validate four legacy series; unrelated JSON retains the generic exporter API.

    Dates are epoch milliseconds at 23:00 Asia/Taipei. Candle prices are index
    points and volume is contracts. TX strategy amounts are 100 million TWD,
    followed by signed net contracts. MTX preserves string net contracts for
    DEA/INV/FOR and a numeric five-day average in contracts.
    """
    name = Path(output_path).name
    if name not in {'FUT_TX.json', 'FUT_MTX.json', 'data.json', 'data_MTX.json'}:
        return
    if not isinstance(data, list) or not data:
        raise ValueError(f'{name} must contain nonempty daily rows')
    candle = name.startswith('FUT_')
    previous = None
    taipei = timezone(timedelta(hours=8))
    for index, row in enumerate(data):
        label = f'{name} row {index}'
        if not isinstance(row, list) or len(row) != (6 if candle else 5):
            raise ValueError(f'{label} has invalid column count')
        timestamp = row[0]
        if type(timestamp) is not int or timestamp <= 0:
            raise ValueError(f'{label} requires integer epoch milliseconds')
        try:
            day = datetime.fromtimestamp(timestamp / 1000, taipei)
        except (ValueError, OverflowError, OSError) as exc:
            raise ValueError(f'{label} has invalid date') from exc
        if (day.hour, day.minute, day.second, day.microsecond) != (23, 0, 0, 0):
            raise ValueError(f'{label} date must be 23:00 Asia/Taipei')
        if previous is not None and timestamp <= previous:
            raise ValueError(f'{label} dates must be sorted and unique')
        previous = timestamp
        if candle:
            for value in row[1:5]:
                _number(value, f'{label} price')
                if value < 0:
                    raise ValueError(f'{label} price must be nonnegative')
            opening, high, low, close = row[1:5]
            if not low <= min(opening, close) <= max(opening, close) <= high:
                raise ValueError(f'{label} violates OHLC relationships')
            _number(row[5], f'{label} volume', integer=True)
            if row[5] < 0:
                raise ValueError(f'{label} volume must be nonnegative')
        elif name == 'data.json':
            for column in (1, 2, 3):
                # Only the first delta lacks a preceding position for calculation.
                if column == 1 and index == 0 and row[column] is None:
                    continue
                _number(row[column], f'{label} amount')
            _number(row[4], f'{label} net position', integer=True)
        else:
            for value in row[1:4]:
                if not isinstance(value, str) or not re.fullmatch(r'-?\d+', value):
                    raise ValueError(f'{label} net positions must be integer strings')
            _number(row[4], f'{label} moving average')
