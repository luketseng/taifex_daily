"""Regression checks for repeatable and atomic web JSON publication."""
import json
import logging
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import mining_rpt
from get_data import TaifexDataParser
from lib.db_utils import DBUtils
from lib.export_utils import ExportUtils

LOG = logging.getLogger('json-export-tests')
LOG.addHandler(logging.NullHandler())
mining_rpt.LOGGER = LOG


class JsonExportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        previous = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, previous)

    def test_atomic_failure_preserves_published_json(self):
        path = self.root / 'data.json'
        path.write_text('[1]\n')
        with self.assertRaises(TypeError):
            ExportUtils(LOG).export_to_json([2, object()], str(path))
        self.assertEqual(json.loads(path.read_text()), [1])
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ['data.json'])

    def test_daily_candle_export_overwrites_deduplicates_and_repeats(self):
        database = self.root / 'market.db'
        with sqlite3.connect(database) as connection:
            connection.execute('CREATE TABLE twTX(Date,Time,Open,High,Low,Close,Volume)')
            connection.executemany('INSERT INTO twTX VALUES (?,?,?,?,?,?,?)', [
                ('2026/10/19', '08:46:00', 100, 110, 90, 105, 10),
                ('2026/10/19', '13:45:00', 105, 120, 95, 115, 20),
            ])
        timestamp = int(datetime(2026, 10, 19, 23).timestamp() * 1000)
        old = [timestamp - 86400000, 1, 2, 0, 1, 10]
        stale = [timestamp, 0, 0, 0, 0, 0]
        Path('FUT_TX.json').write_text(json.dumps([stale, old, old]))
        miner = mining_rpt.TaifexReportMiner.__new__(mining_rpt.TaifexReportMiner)
        miner.db_utils = DBUtils(db_path=str(database))
        miner.exporter = ExportUtils(LOG)
        miner.report_info = {'symbol': ['TX']}
        def export():
            miner.export_data_to_txt(symbol='TX', interval=1,
                start_date=datetime(2026, 10, 19), end_date=datetime(2026, 10, 19), session='day')
        export()
        rows = json.loads(Path('FUT_TX.json').read_text())
        self.assertEqual(rows, [old, [timestamp, 100, 120, 90, 115, 30]])
        published = Path('FUT_TX.json').read_bytes()
        export()
        self.assertEqual(published, Path('FUT_TX.json').read_bytes())

    def test_strategy_exports_use_dates_products_and_institution_keys(self):
        market = self.root / 'market.db'
        institutions = self.root / 'institutions.db'
        with sqlite3.connect(market) as connection:
            connection.execute('CREATE TABLE twTX(Date,Time,Close)')
            connection.executemany('INSERT INTO twTX VALUES (?,?,?)', [
                ('2026/01/06', '13:45:00', 110000), ('2026/01/02', '13:45:00', 100000),
                ('2026/01/05', '13:45:00', 105000),
            ])
        with sqlite3.connect(institutions) as connection:
            connection.execute('CREATE TABLE II_Fut(Date,Fut,Institutional,OI_Net_Contract)')
            connection.execute('CREATE TABLE II_SPOT(Date,Institutional,TR_Net_Amount)')
            connection.execute('CREATE TABLE II_OP(Date,OP,PC,Institutional,OI_B_Contract,OI_S_Contract,OI_B_Amount,OI_S_Amount)')
            connection.executemany('INSERT INTO II_Fut VALUES (?,?,?,?)', [
                ('2026/01/06', 'TX', 'FOR', -10), ('2026/01/02', 'TX', 'FOR', -30),
                ('2026/01/05', 'TX', 'FOR', -20),
            ])
            for day in ['2026/01/06', '2026/01/02', '2026/01/05']:
                connection.execute('INSERT INTO II_SPOT VALUES (?,?,?)', (day, 'FOR', 200000000))
                connection.executemany('INSERT INTO II_OP VALUES (?,?,?,?,?,?,?,?)', [
                    (day, 'TXO', 'PUT', 'FOR', 1, 2, 300, 400),
                    (day, 'TXO', 'CALL', 'FOR', 1, 2, 1000, 200),
                    (day, 'TEO', 'CALL', 'FOR', 1, 2, 9999999, 0),
                ])
                connection.executemany('INSERT INTO II_Fut VALUES (?,?,?,?)', [
                    (day, 'MTX', 'FOR', 3), (day, 'MTX', 'DEA', 1),
                ])
                if day != '2026/01/05':
                    connection.execute('INSERT INTO II_Fut VALUES (?,?,?,?)', (day, 'MTX', 'INV', 2))
        parser = TaifexDataParser(LOG)
        parser.market_db_path = market
        parser.ii_db_path = institutions
        parser.run_trading_strategy()
        ts = lambda day: int(datetime.strptime(day, '%Y/%m/%d').replace(hour=23).timestamp() * 1000)
        self.assertEqual(json.loads(Path('data.json').read_text()), [
            [ts('2026/01/02'), None, 2.0, 0.01, -30],
            [ts('2026/01/05'), 2.1, 2.0, 0.01, -20],
            [ts('2026/01/06'), 2.2, 2.0, 0.01, -10],
        ])
        self.assertEqual(json.loads(Path('data_MTX.json').read_text()), [
            [ts('2026/01/02'), '1', '2', '3', 0.0],
            [ts('2026/01/06'), '1', '2', '3', 0.0],
        ])
        published = Path('data.json').read_bytes()
        parser.run_trading_strategy()
        self.assertEqual(published, Path('data.json').read_bytes())
        # Correcting historical MTX data must update subsequent moving averages.
        with sqlite3.connect(institutions) as connection:
            for day, value in [('2026/01/07', 3), ('2026/01/08', 4), ('2026/01/09', 5), ('2026/01/12', 6)]:
                for institution in ['FOR', 'INV', 'DEA']:
                    connection.execute('INSERT INTO II_Fut VALUES (?,?,?,?)', (day, 'MTX', institution, value))
        parser.run_trading_strategy()
        self.assertEqual(json.loads(Path('data_MTX.json').read_text())[-1][-1], 12.0)
        with sqlite3.connect(institutions) as connection:
            connection.execute("UPDATE II_Fut SET OI_Net_Contract=11 WHERE Fut='MTX' AND Date='2026/01/06' AND Institutional='DEA'")
        parser.run_trading_strategy()
        self.assertEqual(json.loads(Path('data_MTX.json').read_text())[-1][-1], 14.0)

    def test_replace_failure_and_symlink_publication(self):
        target = self.root / 'target.json'
        target.write_text('[1]')
        target.chmod(0o664)
        link = self.root / 'linked.json'
        link.symlink_to(target)
        exporter = ExportUtils(LOG)
        with patch('lib.export_utils.os.replace', side_effect=OSError('blocked')):
            with self.assertRaises(OSError):
                exporter.export_to_json([2], str(link))
        self.assertEqual(target.read_text(), '[1]')
        self.assertTrue(link.is_symlink())
        exporter.export_to_json([2], str(link))
        self.assertEqual(json.loads(target.read_text()), [2])
        self.assertTrue(link.is_symlink())
        self.assertEqual(target.stat().st_mode & 0o777, 0o664)
        self.assertFalse(list(self.root.glob('.target.json-*.tmp')))


if __name__ == '__main__':
    unittest.main()
