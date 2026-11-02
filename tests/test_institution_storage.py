"""Exercise institution replacement against disposable SQLite databases."""
import logging
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from get_data import TaifexDataParser


class InstitutionStorageTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name) / 'ii.db'
        self.parser = TaifexDataParser(logging.getLogger(__name__))
        self.parser.ii_db_path = self.path
        self.parser.item = 'SPOT'
        with sqlite3.connect(self.path) as conn:
            conn.execute('CREATE TABLE II_SPOT(Date, Institutional, Buy, Sell, Net CHECK(Net>=0))')
            conn.executemany('INSERT INTO II_SPOT VALUES (?,?,?,?,?)', [
                ('2026/10/22', 'FOR', 10, 2, 8),
                ('2026/10/21', 'FOR', 9, 2, 7),
            ])

    def rows(self):
        with sqlite3.connect(self.path) as conn:
            return conn.execute('SELECT * FROM II_SPOT ORDER BY Date').fetchall()

    def test_invalid_input_preserves_old_rows(self):
        original = self.rows()
        for values in ['', 'broken', "('2026/10/22','FOR',1)", "('2026/10/21','FOR',3,2,1)"]:
            with self.subTest(values=values):
                with self.assertRaises((ValueError, RuntimeError)):
                    self.parser._store_data_in_db('2026/10/22', values)
                self.assertEqual(self.rows(), original)

    def test_partial_insert_failure_rolls_back_delete_and_insert(self):
        original = self.rows()
        with self.assertRaises(RuntimeError):
            self.parser._store_data_in_db('2026/10/22',
                "('2026/10/22','FOR',5,2,3),('2026/10/22','INV',1,2,-1)")
        self.assertEqual(self.rows(), original)

    def test_successful_replacement_is_repeatable(self):
        for _ in range(2):
            self.parser._store_data_in_db('2026/10/22', "('2026/10/22','FOR',5,2,3)")
        self.assertEqual(self.rows(), [('2026/10/21', 'FOR', 9, 2, 7), ('2026/10/22', 'FOR', 5, 2, 3)])
