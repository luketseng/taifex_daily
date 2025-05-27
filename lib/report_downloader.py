#!/usr/bin/python3
import os
import subprocess
import zipfile
from pathlib import Path
from typing import Optional
from lib.log_util import LoggerUtil


class ReportDownloader:
    """
    Handles downloading, verifying, and extracting report ZIP files.
    """

    def __init__(self, report_info: dict):
        """
        Initialize with a logger and a report info dict.

        Args:
            logger (logging.Logger): Logger instance for logging.
            report_info (dict): Dictionary containing report config (must contain 'filename', 'url', 'rptdirpath').
        """
        self.logger = LoggerUtil(name=__name__).get_logger()
        self.report_info = report_info

    def download_report(self, recover: bool = False) -> Path:
        """
        Download the report ZIP file from the specified URL.

        Args:
            recover (bool): Force download even if file exists.

        Returns:
            Path to the downloaded ZIP file.
        """
        # Create report directory if not exists
        report_dir = Path(self.report_info["rptdirpath"])
        report_dir.mkdir(exist_ok=True)

        # Destination file path
        dest_path = report_dir / self.report_info["filename"]

        # Skip download if file exists and not in recover mode
        if not recover and dest_path.exists():
            self.logger.info(f"File already exists: {dest_path}")
            return dest_path

        # Download the file
        url = f"{self.report_info['url']}/{self.report_info['filename']}"
        self.logger.info(f"Downloading {dest_path} from {url}")

        tmp_dir = dest_path.parent
        tmp_file = tmp_dir / Path(url).name

        try:
            # Use subprocess instead of os.system for better error handling
            result = subprocess.run(
                # ["wget", "-O", str(dest_path), url],
                ["wget", "-N", url],
                cwd=tmp_dir,
                capture_output=True,
                text=True,
                check=True,
            )
            self.logger.debug(f"wget output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Download failed: {e.stderr}")
            if dest_path.exists():
                dest_path.unlink()  # Remove failed download
            raise RuntimeError(f"Failed to download report: {e}")

        # Rename if needed
        if tmp_file != dest_path:
            tmp_file.rename(dest_path)

        # Verify downloaded ZIP file
        self.verify_zip_file(dest_path)
        return dest_path

    def verify_zip_file(self, file_path: Path) -> bool:
        """
        Verify that a ZIP file is valid.

        Args:
            file_path (Path): Path to the ZIP file.

        Returns:
            bool: True if file is valid, raises exception otherwise.
        """
        try:
            with zipfile.ZipFile(file_path, "r") as zip_file:
                zip_file.testzip()
            self.logger.info(f"Successfully verified ZIP file: {file_path}")
            return True
        except zipfile.BadZipFile:
            self.logger.warning(f"Invalid ZIP file: {file_path}")
            if file_path.exists():
                file_path.unlink()
            raise ValueError(f"Downloaded file is not a valid ZIP: {file_path}")

    def extract_report(
        self,
        zip_path: Optional[Path] = None,
        extract_dir: Optional[Path] = None,
    ) -> Path:
        """
        Extract the report ZIP file.

        Args:
            zip_path (Path, optional): Path to the ZIP file.
            extract_dir (Path, optional): Directory to extract to (defaults to tmp subdirectory).

        Returns:
            Path to directory containing extracted files.
        """
        # Use provided zip_path or construct from report info
        if zip_path is None:
            zip_path = Path(self.report_info["rptdirpath"]) / self.report_info["filename"]

        # Default extract directory is tmp subdirectory of report directory
        if extract_dir is None:
            extract_dir = Path(self.report_info["rptdirpath"]) / "tmp"

        # Create extract directory if not exists
        extract_dir.mkdir(exist_ok=True)

        # Check if zip file exists
        if not zip_path.exists():
            self.logger.error(f"ZIP file not found: {zip_path}")
            raise FileNotFoundError(f"ZIP file not found: {zip_path}")

        # Extract files
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_file:
                zip_file.extractall(extract_dir)
                extracted_files = zip_file.namelist()

            self.logger.info(f"Extracted {len(extracted_files)} files from {zip_path} to {extract_dir}")
            return extract_dir
        except Exception as e:
            self.logger.error(f"Failed to extract ZIP file: {e}")
            raise RuntimeError(f"Failed to extract ZIP file: {e}")

    def extract_all_reports(self) -> Path:
        """
        Extract all ZIP files in the report directory.

        Returns:
            Path to the extraction directory.
        """
        self.logger.info(f"Extracting all reports in {self.report_info['rptdirpath']}")

        report_dir = Path(self.report_info["rptdirpath"])
        extract_dir = report_dir / "tmp"
        extract_dir.mkdir(exist_ok=True)

        if not report_dir.is_dir():
            self.logger.warning(f"Report directory not found: {report_dir}")
            return extract_dir

        # Process each ZIP file in the directory
        zip_count = 0
        for file_path in report_dir.glob("*.zip"):
            try:
                self.verify_zip_file(file_path)
                with zipfile.ZipFile(file_path, "r") as zip_file:
                    for filename in zip_file.namelist():
                        zip_file.extract(filename, extract_dir)
                        self.logger.debug(f"Extracted {filename} to {extract_dir}")
                zip_count += 1
            except (zipfile.BadZipFile, Exception) as e:
                self.logger.warning(f"Skipping {file_path}: {e}")
                continue

        self.logger.info(f"Extracted {zip_count} ZIP files to {extract_dir}")
        return extract_dir

