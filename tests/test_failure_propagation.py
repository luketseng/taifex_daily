"""Failure boundaries must stop publication and preserve meaningful CLI status."""
import logging
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import get_data
import mining_rpt


class FailurePropagationTests(unittest.TestCase):
    def test_day_query_failure_is_not_empty_day(self):
        miner = mining_rpt.TaifexReportMiner.__new__(mining_rpt.TaifexReportMiner)
        miner.db_utils = Mock()
        miner.db_utils.db_execute_query.return_value = None
        with self.assertRaises(RuntimeError):
            miner._get_day_rows('twTX', '2026/10/22')
        miner.db_utils.db_execute_query.return_value = []
        self.assertEqual(miner._get_day_rows('twTX', '2026/10/22'), [])

    def test_fetch_failure_propagates(self):
        parser = get_data.TaifexDataParser(logging.getLogger(__name__))
        with patch.object(parser, 'find_chromedriver', return_value='/fake'), \
             patch('get_data.Chrome') as chrome:
            chrome.return_value.__enter__.return_value.get.side_effect = RuntimeError('network failed')
            with self.assertRaises(RuntimeError):
                parser.fetch_data_from_web('SPOT', '2026/10/22')

    def test_failed_fetch_stops_remaining_pipeline(self):
        with patch('get_data.parse_args') as arguments, \
             patch('get_data.TaifexDataParser') as parser_type, \
             patch('get_data.export_foreign_positions') as positions:
            arguments.return_value.date = None
            arguments.return_value.log_level = 'ERROR'
            parser = parser_type.return_value
            parser.fetch_data_from_web.side_effect = RuntimeError('fetch failed')
            with self.assertRaises(RuntimeError):
                get_data.main()
            self.assertEqual(parser.fetch_data_from_web.call_count, 1)
            parser.run_trading_strategy.assert_not_called()
            positions.assert_not_called()

    def test_failed_position_export_does_not_report_success(self):
        with patch('get_data.parse_args') as arguments, \
             patch('get_data.TaifexDataParser'), \
             patch('get_data.export_foreign_positions', return_value=False):
            arguments.return_value.date = None
            arguments.return_value.log_level = 'ERROR'
            with self.assertRaises(RuntimeError):
                get_data.main()

    def test_miner_cli_export_failure_exits_nonzero(self):
        with patch('mining_rpt.parse_arguments') as arguments, \
             patch('mining_rpt.TaifexReportMiner') as miner:
            arguments.return_value.export = True
            arguments.return_value.log_level = 'ERROR'
            miner.return_value.export_data_to_txt.side_effect = RuntimeError('query failed')
            with self.assertRaises(SystemExit) as result:
                mining_rpt.main()
            self.assertEqual(result.exception.code, 1)

    def test_unrecognized_empty_response_is_failure(self):
        parser = get_data.TaifexDataParser(logging.getLogger(__name__))
        with patch.object(parser, 'find_chromedriver', return_value='/fake'), \
             patch('get_data.Chrome'), \
             patch.object(parser, '_fetch_spot_data'):
            with self.assertRaises(RuntimeError):
                parser.fetch_data_from_web('SPOT', '2026/10/22')

    def test_older_valid_result_is_no_data_for_requested_day(self):
        parser = get_data.TaifexDataParser(logging.getLogger(__name__))
        parser.date = '2026/10/22'
        parser.lines_data = ['2026/10/21']
        with self.assertRaises(get_data.NoDataAvailable):
            parser._prepare_data_for_db('Fut')

    def test_explicit_no_data_response_stops_fetch_without_db_write(self):
        parser = get_data.TaifexDataParser(logging.getLogger(__name__))
        for item, text in [('SPOT', '很抱歉，沒有符合條件的資料!'), ('Fut', '查無資料'), ('OP', '查無符合條件的資料')]:
            with self.subTest(item=item), \
                 patch.object(parser, 'find_chromedriver', return_value='/fake'), \
                 patch('get_data.Chrome') as chrome, \
                 patch('get_data.WebDriverWait') as wait, \
                 patch.object(parser, '_store_data_in_db') as store:
                browser = chrome.return_value.__enter__.return_value
                browser.find_element.return_value.text = text
                wait.return_value.until.side_effect = lambda callback: callback(browser)
                with self.assertRaises(get_data.NoDataAvailable):
                    parser.fetch_data_from_web(item, '2026/10/22')
                store.assert_not_called()

    def test_missing_or_future_result_date_is_failure(self):
        parser = get_data.TaifexDataParser(logging.getLogger(__name__))
        parser.date = '2026/10/22'
        for result in ['changed layout', '2026/10/23', '2026/99/99']:
            with self.subTest(result=result):
                parser.lines_data = [result]
                with self.assertRaises(ValueError):
                    parser._prepare_data_for_db('Fut')

    def test_spot_older_date_is_no_data(self):
        parser = get_data.TaifexDataParser(logging.getLogger(__name__))
        parser.date = '2026/10/22'
        parser.lines_data = [['115年10月21日', 'title'], ['header'], ['total']]
        with self.assertRaises(get_data.NoDataAvailable):
            parser._prepare_data_for_db('SPOT')

    def test_no_data_at_any_fetch_stage_exits_two_and_preserves_files(self):
        import os
        import tempfile
        for stage in range(3):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                previous = Path.cwd()
                try:
                    os.chdir(directory)
                    outputs = ['data.json', 'data_MTX.json', 'foreign_positions.json']
                    for output in outputs:
                        Path(output).write_text('["published snapshot"]')
                    with patch('get_data.parse_args') as arguments, \
                         patch('get_data.TaifexDataParser') as parser_type, \
                         patch('get_data.export_foreign_positions') as positions:
                        arguments.return_value.date = None
                        arguments.return_value.log_level = 'ERROR'
                        parser = parser_type.return_value
                        parser.fetch_data_from_web.side_effect = [None] * stage + [get_data.NoDataAvailable('not published')]
                        with self.assertRaises(SystemExit) as result:
                            get_data.main()
                        self.assertEqual(result.exception.code, 2)
                        self.assertEqual(parser.fetch_data_from_web.call_count, stage + 1)
                        parser.run_trading_strategy.assert_not_called()
                        positions.assert_not_called()
                        for output in outputs:
                            self.assertEqual(Path(output).read_text(), '["published snapshot"]')
                finally:
                    os.chdir(previous)

    def test_successful_daily_run_completes_all_stages(self):
        with patch('get_data.parse_args') as arguments, \
             patch('get_data.TaifexDataParser') as parser_type, \
             patch('get_data.export_foreign_positions', return_value=True) as positions:
            arguments.return_value.date = None
            arguments.return_value.log_level = 'ERROR'
            self.assertIsNone(get_data.main())
            self.assertEqual(parser_type.return_value.fetch_data_from_web.call_count, 3)
            parser_type.return_value.run_trading_strategy.assert_called_once()
            positions.assert_called_once()
