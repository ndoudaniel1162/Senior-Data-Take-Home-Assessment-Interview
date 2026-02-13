import json
import os
import glob
import logging
from datetime import datetime
from typing import List, Dict, Any

from storage.database_simple import DatabaseSimple
from quality.validator import TelemetryValidator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migration")


class LakeToOperationalMigrator:
    """
    Migrates historical telemetry data from JSON lake
    into operational SQLite time-series tables.
    """

    def __init__(self, lake_root: str = "data/lake", db_path: str = "iot_telemetry.db"):
        self.lake_root = lake_root
        self.db = DatabaseSimple(db_path)
        self.validator = TelemetryValidator()

    # ---------- public ----------

    def migrate_all(self) -> None:
        files = self._discover_lake_files()

        total = 0
        success = 0
        failed = 0

        logger.info(f"Found {len(files)} lake files")

        for file_path in files:
            records = self._read_file(file_path)

            for record in records:
                total += 1

                if not self.validator.is_valid(record):
                    failed += 1
                    continue

                try:
                    self.db.insert_telemetry(record)
                    success += 1
                except Exception as e:
                    logger.error(f"DB insert failed: {e}")
                    failed += 1

        logger.info("====== MIGRATION SUMMARY ======")
        logger.info(f"Total scanned: {total}")
        logger.info(f"Inserted: {success}")
        logger.info(f"Failed: {failed}")

    # ---------- helpers ----------

    def _discover_lake_files(self) -> List[str]:
        pattern = os.path.join(self.lake_root, "**", "*.json")
        return glob.glob(pattern, recursive=True)

    def _read_file(self, path: str) -> List[Dict[str, Any]]:
        rows = []
        with open(path, "r") as f:
            for line in f:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning(f"Bad JSON line in {path}")
        return rows


if __name__ == "__main__":
    migrator = LakeToOperationalMigrator()
    migrator.migrate_all()