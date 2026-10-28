"""Exercise the production storage method with isolated SQLite databases."""
import ast
import logging
from pathlib import Path
import sqlite3
import sys
import tempfile
from typing import List, Tuple
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from lib.db_utils import DBUtils

# Load the real method without browser and Google Drive dependencies.
source = ast.parse((ROOT / "mining_rpt.py").read_text())
klass = next(n for n in source.body if isinstance(n, ast.ClassDef) and n.name == "TaifexReportMiner")
method = next(n for n in klass.body if isinstance(n, ast.FunctionDef) and n.name == "_store_candles_in_db")
LOG = logging.getLogger(__name__)
LOG.addHandler(logging.NullHandler())
namespace = {"List": List, "Tuple": Tuple, "LOGGER": LOG}
exec(compile(ast.Module(body=[method], type_ignores=[]), str(ROOT / "mining_rpt.py"), "exec"), namespace)
Miner = type("Miner", (), {"_store_candles_in_db": namespace[method.name]})


def candle(date, time, close=100):
    return (date, time, 100, 101, 99, close, 1)


class CandleStorageTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name) / "test.db"
        with sqlite3.connect(self.path) as conn:
            for symbol in ("TX", "MTX"):
                conn.execute(f"CREATE TABLE tw{symbol}(Date,Time,Open,High,Low,Close,Volume)")
        self.miner = Miner()
        self.miner.report_info = {"symbol": ["TX", "MTX"]}
        self.miner.db_utils = DBUtils(logger=LOG, db_path=self.path)

    def seed(self, rows, table="twTX"):
        with sqlite3.connect(self.path) as conn:
            conn.executemany(f"INSERT INTO {table} VALUES (?,?,?,?,?,?,?)", rows)

    def rows(self, table="twTX"):
        with sqlite3.connect(self.path) as conn:
            return conn.execute(f"SELECT * FROM {table} ORDER BY Date,Time").fetchall()

    def test_missing_open_cross_holiday_and_unsorted_replay(self):
        rows = [candle("2026/10/27", "08:46:00"),
                candle("2026/10/22", "15:02:00"),
                candle("2026/10/23", "00:02:00")]
        for symbol in ("TX", "MTX"):
            for _ in range(2):
                self.assertTrue(self.miner._store_candles_in_db(rows, symbol))
            self.assertEqual(self.rows(f"tw{symbol}"), sorted(rows))

    def test_updates_target_keys_and_preserves_missing_minutes_and_other_rows(self):
        target = candle("2026/10/22", "15:02:00")
        keep = [candle("2026/10/22", "15:01:00"),
                candle("2026/10/22", "08:46:00"),
                candle("2026/10/23", "00:01:00"),
                candle("2026/10/28", "08:46:00"), (None,) * 7]
        self.seed([target, target] + keep)
        self.seed([target], "twMTX")
        updated = candle(*target[:2], close=101)
        self.assertTrue(self.miner._store_candles_in_db([updated], "TX"))
        self.assertCountEqual(self.rows(), keep + [updated])
        self.assertEqual(self.rows("twMTX"), [target])

    def test_insert_failure_rolls_back_deletions_and_partial_insert(self):
        old = candle("2026/10/22", "15:01:00")
        self.seed([old, old])
        with sqlite3.connect(self.path) as conn:
            conn.execute("CREATE TRIGGER reject_bad BEFORE INSERT ON twTX WHEN NEW.Close=999 BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
        incoming = [candle(*old[:2], close=101), candle("2026/10/23", "00:01:00", close=999)]
        self.assertFalse(self.miner._store_candles_in_db(incoming, "TX"))
        self.assertEqual(self.rows(), [old, old])

    def test_duplicate_or_empty_input_keys_are_rejected_before_deletion(self):
        old = candle("2026/10/22", "15:01:00")
        self.seed([old])
        for rows in ([old, old], [candle(None, "15:01:00")], [candle("2026/10/22", "")]):
            self.assertFalse(self.miner._store_candles_in_db(rows, "TX"))
            self.assertEqual(self.rows(), [old])

    def test_empty_input_preserves_database(self):
        old = candle("2026/10/22", "15:01:00")
        self.seed([old])
        self.assertFalse(self.miner._store_candles_in_db([], "TX"))
        self.assertEqual(self.rows(), [old])


if __name__ == "__main__":
    unittest.main()
