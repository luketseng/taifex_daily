"""Real CLI orchestration must report partial report failures to cron."""
from argparse import Namespace
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import mining_rpt


class MiningPipelineStatusTests(unittest.TestCase):
    @contextmanager
    def pipeline(self, days=1):
        start = datetime(2026, 11, 4)
        arguments = Namespace(date='20261104', export=None, recover=True, log_level='INFO')
        initial = Mock()
        initial._validate_date_range.return_value = (start, start+timedelta(days=days-1))
        jobs = [Mock() for _ in range(days*2)]
        for job in jobs:
            job.report_info = {'symbol': ['TX', 'MTX']}
            job.upload_to_gdrive.return_value = True
            job.parse_report_to_db.return_value = True
        logger = Mock()
        with patch.object(mining_rpt, 'parse_arguments', return_value=arguments), \
                patch.object(mining_rpt, 'LoggerUtil') as log_util, \
                patch.object(mining_rpt, 'TaifexReportMiner', side_effect=[initial, *jobs]), \
                patch.object(mining_rpt, 'LOGGER', create=True), \
                patch.object(mining_rpt, 'ITEMS', ['fut_rpt', 'opt_rpt']):
            log_util.return_value.get_logger.return_value = logger
            yield jobs, logger

    def assert_failed(self, logger):
        with self.assertRaises(SystemExit) as result:
            mining_rpt.main()
        self.assertEqual(result.exception.code, 1)
        self.assertFalse(any('completed successfully' in str(call)
                             for call in logger.info.call_args_list))
        self.assertTrue(logger.error.called)

    def test_success_keeps_the_existing_pipeline_order(self):
        with self.pipeline() as (jobs, logger):
            self.assertIsNone(mining_rpt.main())
            for job in jobs:
                job.download_report.assert_called_once_with(recover=True)
                job.upload_to_gdrive.assert_called_once_with(recover=True)
            self.assertEqual([call.args[0] for call in jobs[0].parse_report_to_db.call_args_list], ['TX', 'MTX'])
            jobs[1].parse_report_to_db.assert_not_called()
            logger.info.assert_any_call('TAIFEX data mining completed successfully')

    def test_failed_backup_is_failure_but_valid_data_can_still_be_imported(self):
        for item in (0, 1):
            with self.subTest(item=item), self.pipeline() as (jobs, logger):
                jobs[item].upload_to_gdrive.return_value = False
                self.assert_failed(logger)
                self.assertEqual(jobs[0].parse_report_to_db.call_count, 2)
                jobs[1].upload_to_gdrive.assert_called_once()

    def test_false_parse_status_does_not_hide_behind_later_successes(self):
        with self.pipeline(days=2) as (jobs, logger):
            jobs[0].parse_report_to_db.side_effect = [False, True]
            self.assert_failed(logger)
            self.assertEqual(jobs[0].parse_report_to_db.call_count, 2)
            self.assertEqual(jobs[2].parse_report_to_db.call_count, 2)
            jobs[3].upload_to_gdrive.assert_called_once()
            self.assertIn('2026_11_04', str(logger.error.call_args_list))
            self.assertIn('TX', str(logger.error.call_args_list))

    def test_parse_exception_keeps_other_symbol_and_report_work_independent(self):
        with self.pipeline() as (jobs, logger):
            jobs[0].parse_report_to_db.side_effect = [RuntimeError('DB failed'), True]
            self.assert_failed(logger)
            self.assertEqual(jobs[0].parse_report_to_db.call_count, 2)
            jobs[1].upload_to_gdrive.assert_called_once()

    def test_download_failure_skips_that_report_and_reports_failure(self):
        with self.pipeline() as (jobs, logger):
            jobs[0].download_report.side_effect = RuntimeError('source unavailable')
            self.assert_failed(logger)
            jobs[0].upload_to_gdrive.assert_not_called()
            jobs[0].parse_report_to_db.assert_not_called()
            jobs[1].download_report.assert_called_once()

    def test_upload_exception_and_options_failure_are_not_reported_as_success(self):
        for stage in ('upload_to_gdrive', 'download_report'):
            with self.subTest(stage=stage), self.pipeline() as (jobs, logger):
                getattr(jobs[1], stage).side_effect = RuntimeError('options failed')
                self.assert_failed(logger)


if __name__ == '__main__':
    unittest.main()
