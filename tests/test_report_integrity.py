"""Reject archives whose members fail CRC even when the ZIP opens."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.report_util import ReportUtil


class ReportIntegrityTests(unittest.TestCase):
    def test_corrupt_member_rejected_and_source_retained_for_inspection(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'report.zip'
            with zipfile.ZipFile(path, 'w') as archive:
                archive.writestr('report.rpt', 'ticks')
            original = path.read_bytes()
            with patch.object(zipfile.ZipFile, 'testzip', return_value='report.rpt'):
                with self.assertRaisesRegex(ValueError, 'CRC'):
                    ReportUtil().verify_zip_file(path)
            self.assertEqual(path.read_bytes(), original)


if __name__ == '__main__':
    unittest.main()
