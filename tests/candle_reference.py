"""Independent raw-trade reference supporting TX/MTX parser regression tests."""
from collections import defaultdict
from datetime import datetime, timedelta


def read_report_ticks(path, symbol, report_date):
    """Read current-month outright ticks, falling back exactly as the importer.

    Keep all occurrences. This reader does not invoke the production shell
    filter or parser. report_date uses YYYY_MM_DD.
    """
    date = datetime.strptime(report_date, "%Y_%m_%d")
    months = [date.strftime("%Y%m"), (date + timedelta(weeks=4)).strftime("%Y%m")]
    found = {month: [] for month in months}
    with open(path, errors="replace") as stream:
        for line in stream:
            row = [value.strip() for value in line.split(",")]
            if len(row) >= 8 and row[1] == symbol and row[2] in found:
                found[row[2]].append(row[:8])
    for month in months:
        if found[month]:
            return found[month]
    return []


def reference_candles(ticks):
    """Group every raw record by minute; identical records remain separate trades."""
    groups = defaultdict(list)
    for row in ticks:
        stamp = datetime.strptime(str(row[0]) + str(row[3]), "%Y%m%d%H%M%S")
        # This reference deliberately does not call production parser helpers.
        end = stamp.replace(second=0) + timedelta(minutes=1)
        if str(row[3]) in ("050000", "134500"):
            end = stamp
        groups[end].append((int(row[4]), int(row[5])))
    result = []
    for end, trades in sorted(groups.items()):
        prices = [price for price, quantity in trades]
        result.append((end.strftime("%Y/%m/%d"), end.strftime("%H:%M:%S"),
                       prices[0], max(prices), min(prices), prices[-1],
                       sum(quantity for price, quantity in trades) // 2))
    return result


def validate_session_candles(ticks, candles, night_start_date, trading_date):
    """Require 1140 minus truly empty slots AND exact raw-derived OHLCV.

    Dates are calendar night-start and regular-session dates, in YYYY/MM/DD.
    They need not be consecutive: holidays can separate night and day sessions.
    This audit applies to a standard 840-minute night plus 300-minute day.
    """
    night = datetime.strptime(night_start_date, "%Y/%m/%d").replace(hour=15, minute=1)
    day = datetime.strptime(trading_date, "%Y/%m/%d").replace(hour=8, minute=46)
    slots = [night + timedelta(minutes=i) for i in range(840)]
    slots += [day + timedelta(minutes=i) for i in range(300)]
    allowed = {(stamp.strftime("%Y/%m/%d"), stamp.strftime("%H:%M:%S")) for stamp in slots}
    if len(allowed) != 1140:
        raise ValueError("Night and day dates must identify distinct session slots")
    expected = reference_candles(ticks)
    occupied = {row[:2] for row in expected}
    if occupied - allowed:
        raise ValueError(f"Raw trades outside the expected sessions: {sorted(occupied-allowed)[:5]}")
    actual = [tuple(row) for row in candles]
    keys = [row[:2] for row in actual]
    if len(set(keys)) != len(keys):
        raise ValueError("Duplicate candle minute keys")
    empty = len(allowed - occupied)
    if len(actual) != 1140 - empty:
        raise ValueError(f"Expected {1140-empty} candles (1140 minus {empty} empty minutes), got {len(actual)}")
    if actual != expected:
        raise ValueError("Candle dates/order/OHLCV differ from independent raw-trade grouping")
    return dict(total_session_minutes=1140, zero_trade_minutes=empty,
                expected_candles=1140-empty, actual_candles=len(actual),
                raw_ohlcv_matches=True)
