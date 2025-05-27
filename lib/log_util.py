#!/usr/bin/python3
import logging
import sys
from typing import Optional, Union


class LoggerUtil:
    """
    Logger utility class for flexible and centralized logging configuration.

    Logging level mapping:
        "CRITICAL" or logging.CRITICAL = 50
        "ERROR"    or logging.ERROR    = 40
        "WARNING"  or logging.WARNING  = 30
        "INFO"     or logging.INFO     = 20
        "DEBUG"    or logging.DEBUG    = 10
        "NOTSET"   or logging.NOTSET   = 0

    You can use either string or logging level constant for level.
    """

    LEVEL_MAP = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }

    def __init__(
        self,
        name: Optional[str] = None,
        level: Union[int, str] = logging.INFO,
        fmt: str = "%(asctime)s | %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    ):
        """
        Initialize LoggerUtil instance and configure logger.

        Args:
            name (str, optional): Logger name. Defaults to None (root logger).
            level (int or str, optional): Logging level (e.g. "INFO", "DEBUG" or logging.INFO).
            fmt (str, optional): Format string for log messages.
            stream: Output stream for logs. Defaults to sys.stdout.
        """
        resolved_level = self._resolve_level(level)
        self.logger = logging.getLogger(name)
        self.logger.setLevel(resolved_level)

        if not self.logger.handlers:
            handler = logging.StreamHandler(stream)
            handler.setLevel(resolved_level)
            formatter = logging.Formatter(fmt)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.logger.propagate = False

    def get_logger(self) -> logging.Logger:
        """
        Get the configured logger instance.

        Returns:
            logging.Logger: Logger instance.
        """
        return self.logger

    def add_file_handler(self, file_path: str, level: Union[int, str] = None, fmt: Optional[str] = None):
        """
        Add a file handler for logging to a file.

        Args:
            file_path (str): Path to log file.
            level (int or str, optional): Logging level for file handler (e.g. "DEBUG", logging.INFO).
            fmt (str, optional): Format string for file log messages.
        """
        resolved_level = self._resolve_level(level) if level is not None else self.logger.level
        file_handler = logging.FileHandler(file_path)
        file_handler.setLevel(resolved_level)
        formatter = logging.Formatter(fmt if fmt else "%(asctime)s | %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _resolve_level(self, level: Union[int, str, None]) -> int:
        """
        Resolve level from string or int to logging constant.

        Args:
            level (int or str): Level such as "INFO" or logging.INFO

        Returns:
            int: Corresponding logging level
        """
        if isinstance(level, int):
            return level
        if isinstance(level, str):
            upper = level.upper()
            if upper in self.LEVEL_MAP:
                return self.LEVEL_MAP[upper]
        # Default fallback
        return logging.INFO

