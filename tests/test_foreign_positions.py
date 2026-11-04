"""Run with python3 -m unittest discover -s tests -p test_foreign_positions.py."""
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.foreign_positions import export_foreign_positions


class PositionExportTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = Path(self.directory.name) / 'source.db'
        self.output = Path(self.directory.name) / 'positions.json'
        with sqlite3.connect(self.db) as connection:
            connection.execute('CREATE TABLE II_Fut(Date, Fut, Institutional, OI_B_Contract, OI_S_Contract)')
            connection.execute('CREATE TABLE II_OP(Date, OP, PC, Institutional, OI_B_Contract, OI_S_Contract)')
            connection.execute("INSERT INTO II_Fut VALUES ('2026/10/16','TX','FOR',9812,103487)")
            connection.executemany('INSERT INTO II_OP VALUES (?,?,?,?,?,?)', [
                ('2026/10/16', 'TXO', 'CALL', 'FOR', 15849, 15146),
                ('2026/10/16', 'TXO', 'PUT', 'FOR', 15123, 7803),
            ])

    def export(self, day='2026/10/16'):
        return export_foreign_positions(self.db, day, self.output)

    def test_merge_overwrite_and_permissions(self):
        self.output.write_text(json.dumps({'dates': {'2026-10-15': {'sentinel': True}}}))
        self.assertTrue(self.export())
        first = self.output.read_bytes()
        self.assertTrue(self.export())
        self.assertEqual(first, self.output.read_bytes())
        data = json.loads(first)['dates']
        self.assertEqual(data['2026-10-15'], {'sentinel': True})
        self.assertEqual(data['2026-10-16']['TXO PUT']['sell'], 7803)
        with sqlite3.connect(self.db) as connection:
            connection.execute('UPDATE II_Fut SET OI_B_Contract=10000')
        self.export()
        self.assertEqual(json.loads(self.output.read_text())['dates']['2026-10-16']['TX']['buy'], 10000)
        self.assertEqual(self.output.stat().st_mode & 0o777, 0o644)

    def test_missing_or_partial_day_preserves_file(self):
        self.export()
        before = self.output.read_bytes()
        self.assertFalse(self.export('2026/10/17'))
        with sqlite3.connect(self.db) as connection:
            connection.execute("DELETE FROM II_OP WHERE PC='PUT'")
        self.assertFalse(self.export())
        self.assertEqual(before, self.output.read_bytes())

    def test_corrupt_history_is_not_overwritten(self):
        self.output.write_text('broken')
        with self.assertRaises(ValueError):
            self.export()
        self.assertEqual(self.output.read_text(), 'broken')

    def test_atomic_publish_failure_preserves_history(self):
        self.export()
        before = self.output.read_bytes()
        with patch('lib.foreign_positions.os.replace', side_effect=OSError('publish failed')):
            with self.assertRaises(OSError):
                self.export()
        self.assertEqual(before, self.output.read_bytes())
        self.assertEqual(list(self.output.parent.glob('.foreign_positions-*')), [])

    def test_duplicate_products_preserve_file(self):
        self.export()
        original = self.output.read_bytes()
        for table, predicate in [('II_Fut', 'Fut="TX"'), ('II_OP', 'PC="CALL"'),
                                 ('II_OP', 'PC="PUT"')]:
            with self.subTest(table=table, predicate=predicate):
                with sqlite3.connect(self.db) as connection:
                    connection.execute(f'INSERT INTO {table} SELECT * FROM {table} WHERE {predicate}')
                    duplicate_id = connection.execute('SELECT last_insert_rowid()').fetchone()[0]
                try:
                    with self.assertRaisesRegex(ValueError, 'Duplicate position row'):
                        self.export()
                    self.assertEqual(self.output.read_bytes(), original)
                finally:
                    with sqlite3.connect(self.db) as connection:
                        connection.execute(f'DELETE FROM {table} WHERE rowid=?', (duplicate_id,))

    def test_invalid_counts_preserve_file(self):
        self.export()
        original = self.output.read_bytes()
        for table in ('II_Fut', 'II_OP'):
            for column in ('OI_B_Contract', 'OI_S_Contract'):
                for value in (-1, 1.5, '2', None):
                    with self.subTest(table=table, column=column, value=value):
                        with sqlite3.connect(self.db) as connection:
                            before = connection.execute(f'SELECT rowid, {column} FROM {table}').fetchall()
                            connection.execute(f'UPDATE {table} SET {column}=?', (value,))
                        try:
                            with self.assertRaisesRegex(ValueError, 'Invalid position counts'):
                                self.export()
                            self.assertEqual(self.output.read_bytes(), original)
                        finally:
                            with sqlite3.connect(self.db) as connection:
                                connection.executemany(f'UPDATE {table} SET {column}=? WHERE rowid=?',
                                                       [(count, rowid) for rowid, count in before])


if __name__ == '__main__':
    unittest.main()
