from lib.log_util import LoggerUtil
import json
from typing import List, Dict, Any

class ExportUtils:
    def __init__(self, logger=None):
        self.logger = logger or LoggerUtil(name=__name__).get_logger()

    def export_to_csv(self, header: str, data: List[str], output_path: str) -> str:
        """
        Write data to a CSV file.
        Args:
            header: CSV header string
            data: List of CSV row strings
            output_path: Output file path
        Returns:
            Output file path
        """
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(f"{header}\n")
                for row in data:
                    f.write(f"{row}\n")
            self.logger.info(f"Data exported to CSV: {output_path}")
            return output_path
        except Exception as e:
            self.logger.error(f"Failed to export CSV: {e}")
            raise

    def export_to_json(self, data: Any, output_path: str) -> str:
        """
        Write data to a JSON file.
        Args:
            data: Data to write (list/dict)
            output_path: Output file path
        Returns:
            Output file path
        """
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            self.logger.info(f"Data exported to JSON: {output_path}")
            return output_path
        except Exception as e:
            self.logger.error(f"Failed to export JSON: {e}")
            raise

    def export_strategy_json(self, strategy_data: List, output_path: str) -> str:
        """
        Write strategy data to a JSON file.
        Args:
            strategy_data: List of strategy data
            output_path: Output file path
        Returns:
            Output file path
        """
        return self.export_to_json(strategy_data, output_path)

    def format_db_rows(self, rows: List[tuple], columns: List[str], as_dict: bool = True) -> Any:
        """
        Convert DB query results to list of dicts or list of lists.
        Args:
            rows: List of tuples from DB cursor.fetchall()
            columns: List of column names
            as_dict: If True, return list of dicts; else, list of lists
        Returns:
            List of dicts or lists
        """
        if as_dict:
            formatted = [dict(zip(columns, row)) for row in rows]
        else:
            formatted = [list(row) for row in rows]
        self.logger.debug(f"Formatted {len(rows)} DB rows to {'dict' if as_dict else 'list'} format.")
        return formatted