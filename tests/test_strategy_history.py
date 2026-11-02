"""Missing source dates must not erase published strategy history."""
import json
import logging
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from get_data import TaifexDataParser


class StrategyHistoryTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        previous = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, previous)
        self.parser = TaifexDataParser(logging.getLogger(__name__))
        self.parser.market_db_path = self.root / 'market.db'
        self.parser.ii_db_path = self.root / 'ii.db'
        self.days = ['2026/10/21', '2026/10/22']
        with sqlite3.connect(self.parser.market_db_path) as conn:
            conn.execute('CREATE TABLE twTX(Date, Time, Close)')
            conn.executemany('INSERT INTO twTX VALUES (?, ?, ?)', [(d, '13:45:00', 100) for d in self.days])
        with sqlite3.connect(self.parser.ii_db_path) as conn:
            conn.execute('CREATE TABLE II_Fut(Date, Fut, Institutional, OI_Net_Contract)')
            conn.execute('CREATE TABLE II_SPOT(Date, Institutional, TR_Net_Amount)')
            conn.execute('CREATE TABLE II_OP(Date, OP, PC, Institutional, OI_B_Amount, OI_S_Amount)')
            for day in self.days:
                conn.execute('INSERT INTO II_Fut VALUES (?, ?, ?, ?)', (day, 'TX', 'FOR', 5))
                conn.execute('INSERT INTO II_SPOT VALUES (?, ?, ?)', (day, 'FOR', 100))
                for pc in ('CALL', 'PUT'):
                    conn.execute('INSERT INTO II_OP VALUES (?, ?, ?, ?, ?, ?)', (day, 'TXO', pc, 'FOR', 10, 5))
                for institution in ('DEA', 'INV', 'FOR'):
                    conn.execute('INSERT INTO II_Fut VALUES (?, ?, ?, ?)', (day, 'MTX', institution, 5))
        self.parser.run_trading_strategy()

    def test_tx_missing_each_source_preserves_file(self):
        original = Path('data.json').read_bytes()
        for database, query in [
            (self.parser.market_db_path, 'DELETE FROM twTX WHERE Date=?'),
            (self.parser.ii_db_path, 'DELETE FROM II_Fut WHERE Date=? AND Fut="TX"'),
            (self.parser.ii_db_path, 'DELETE FROM II_SPOT WHERE Date=?'),
            (self.parser.ii_db_path, 'DELETE FROM II_OP WHERE Date=? AND PC="PUT"'),
        ]:
            # Apply to a fresh copy, leaving the original fixture available for each case.
            copy_path = self.root / 'copy.db'
            with sqlite3.connect(database) as src, sqlite3.connect(copy_path) as dst:
                src.backup(dst)
                dst.execute(query, (self.days[0],))
            attribute = 'market_db_path' if database == self.parser.market_db_path else 'ii_db_path'
            setattr(self.parser, attribute, copy_path)
            try:
                with self.assertRaises(ValueError):
                    self.parser.run_trading_strategy()
                self.assertEqual(Path('data.json').read_bytes(), original)
            finally:
                setattr(self.parser, attribute, database)
                copy_path.unlink()

    def test_mtx_incomplete_institution_preserves_file(self):
        original = Path('data_MTX.json').read_bytes()
        with sqlite3.connect(self.parser.ii_db_path) as conn:
            conn.execute('DELETE FROM II_Fut WHERE Fut="MTX" AND Institutional="INV" AND Date=?', (self.days[0],))
        with self.assertRaises(ValueError):
            self.parser.run_trading_strategy()
        self.assertEqual(Path('data_MTX.json').read_bytes(), original)

    def test_same_date_correction_is_allowed(self):
        with sqlite3.connect(self.parser.ii_db_path) as conn:
            conn.execute('UPDATE II_Fut SET OI_Net_Contract=10 WHERE Date=?', (self.days[0],))
        self.parser.run_trading_strategy()
        self.assertEqual(json.loads(Path('data.json').read_text())[0][-1], 10)

    def test_corrupt_existing_file_is_not_overwritten(self):
        Path('data.json').write_text('broken')
        with self.assertRaises(ValueError):
            self.parser.run_trading_strategy()
        self.assertEqual(Path('data.json').read_text(), 'broken')
