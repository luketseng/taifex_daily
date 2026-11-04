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

    def test_duplicate_source_keys_preserve_published_file(self):
        cases = [
            ('II_Fut', 'Fut="TX"', 'data.json', 'Duplicate foreign TX position'),
            ('II_OP', 'PC="CALL"', 'data.json', 'Duplicate foreign TXO position'),
            ('II_SPOT', 'Institutional="FOR"', 'data.json', 'Duplicate foreign spot position'),
            ('II_Fut', 'Fut="MTX" AND Institutional="INV"', 'data_MTX.json', 'Duplicate MTX position'),
        ]
        for table, predicate, output, message in cases:
            with self.subTest(table=table, predicate=predicate):
                original = Path(output).read_bytes()
                with sqlite3.connect(self.parser.ii_db_path) as conn:
                    conn.execute(f'INSERT INTO {table} SELECT * FROM {table} WHERE {predicate} AND Date=?',
                                 (self.days[0],))
                    duplicate_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
                try:
                    with self.assertRaisesRegex(ValueError, message):
                        self.parser.run_trading_strategy()
                    self.assertEqual(Path(output).read_bytes(), original)
                finally:
                    with sqlite3.connect(self.parser.ii_db_path) as conn:
                        conn.execute(f'DELETE FROM {table} WHERE rowid=?', (duplicate_id,))

    def test_duplicate_published_date_preserves_file(self):
        for output in ('data.json', 'data_MTX.json'):
            with self.subTest(output=output):
                original = Path(output).read_bytes()
                rows = json.loads(original)
                Path(output).write_text(json.dumps(rows + [rows[0]]))
                invalid = Path(output).read_bytes()
                try:
                    with self.assertRaisesRegex(ValueError, 'duplicate strategy date'):
                        self.parser.run_trading_strategy()
                    self.assertEqual(Path(output).read_bytes(), invalid)
                finally:
                    Path(output).write_bytes(original)

    def test_distinct_foreign_spot_institutions_are_summed(self):
        with sqlite3.connect(self.parser.ii_db_path) as conn:
            conn.execute('INSERT INTO II_SPOT VALUES (?, ?, ?)',
                         (self.days[0], 'FOR_D', 200000000))
        self.parser.run_trading_strategy()
        self.assertEqual(json.loads(Path('data.json').read_text())[0][2], 2.0)

    def test_explicit_daily_run_requires_all_current_sources_before_publish(self):
        originals = {name: Path(name).read_bytes() for name in ('data.json', 'data_MTX.json')}
        cases = [
            (self.parser.ii_db_path, 'DELETE FROM II_Fut WHERE Date=? AND Fut="TX"'),
            (self.parser.market_db_path, 'DELETE FROM twTX WHERE Date=?'),
            (self.parser.ii_db_path, 'DELETE FROM II_SPOT WHERE Date=?'),
            (self.parser.ii_db_path, 'DELETE FROM II_OP WHERE Date=? AND PC="PUT"'),
            (self.parser.ii_db_path, 'DELETE FROM II_Fut WHERE Date=? AND Fut="MTX" AND Institutional="INV"'),
        ]
        for database, query in cases:
            with self.subTest(query=query):
                copy_path = self.root / 'daily-copy.db'
                with sqlite3.connect(database) as src, sqlite3.connect(copy_path) as dst:
                    src.backup(dst)
                    dst.execute(query, (self.days[-1],))
                attribute = 'market_db_path' if database == self.parser.market_db_path else 'ii_db_path'
                setattr(self.parser, attribute, copy_path)
                # No existing current-day row can mask an incomplete first publication.
                for name, payload in originals.items():
                    Path(name).write_text(json.dumps(json.loads(payload)[:-1]))
                before = {name: Path(name).read_bytes() for name in originals}
                try:
                    with self.assertRaisesRegex(ValueError, 'Incomplete .* strategy sources'):
                        self.parser.run_trading_strategy(self.days[-1])
                    for name in originals:
                        self.assertEqual(Path(name).read_bytes(), before[name])
                finally:
                    setattr(self.parser, attribute, database)
                    copy_path.unlink()
                    for name, payload in originals.items():
                        Path(name).write_bytes(payload)

    def test_explicit_daily_run_with_complete_sources_succeeds(self):
        self.parser.run_trading_strategy(self.days[-1])
        self.assertEqual(len(json.loads(Path('data.json').read_text())), 2)
        self.assertEqual(len(json.loads(Path('data_MTX.json').read_text())), 2)
