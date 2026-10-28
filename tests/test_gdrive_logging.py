"""Check that optional upload lookups do not generate false warnings."""
import sys
from pathlib import Path
import unittest
from unittest.mock import MagicMock, Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from devices.gdrive2 import gdrive


class DriveLoggingTests(unittest.TestCase):
    def setUp(self):
        # Bypass authentication and use an in-memory Drive API substitute.
        self.client = gdrive.__new__(gdrive)
        self.client.logger = Mock()
        self.client.drive = MagicMock()
        self.client.item_obj = {"fut_rpt": {"id": "test-folder", "title": "fut_rpt"}}

    def test_first_upload_is_info_and_still_uploads(self):
        self.client.drive.ListFile.return_value.GetList.return_value = []
        self.client.UploadFile("/tmp/Daily_test.zip", "fut_rpt")
        self.client.logger.warning.assert_not_called()
        self.client.logger.info.assert_any_call(
            "'Daily_test.zip' does not exist in Google Drive yet; creating a new file"
        )
        self.client.drive.CreateFile.return_value.Upload.assert_called_once_with()

    def test_required_lookup_still_warns_when_missing(self):
        self.client.drive.ListFile.return_value.GetList.return_value = []
        self.assertIsNone(self.client.getObjByName("rpt"))
        self.client.logger.warning.assert_called_once()

    def test_duplicate_lookup_still_warns_during_upload(self):
        self.client.drive.ListFile.return_value.GetList.return_value = [MagicMock(), MagicMock()]
        self.client.UploadFile("/tmp/Daily_test.zip", "fut_rpt")
        self.client.logger.warning.assert_called_once()


if __name__ == "__main__":
    unittest.main()
