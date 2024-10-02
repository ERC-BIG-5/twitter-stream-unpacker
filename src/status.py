import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from src.consts import MAIN_STATUS_FILE_PATH, CONFIG, BASE_STAT_PATH, logger
from src.util import year_month_str

@dataclass(frozen=True)
class YearMonth:
    year: int
    month: int

    def __str__(self):
        return f"{self.year:04d}-{self.month:02d}"



class MonthDatasetStatus(BaseModel):
    key: YearMonth
    folder_name: str
    valid: Optional[bool] = False
    annotated_db_available: bool = False
    index_db_available: bool = False
    stats_file_available: bool = False
    label_studio_project_created: bool = False

    @property
    def stats_file_path(self) -> Path:
        ym_str = f"{year_month_str(self.key.year, self.key.month)}.json"
        return BASE_STAT_PATH / ym_str


class MainStatus(BaseModel):
    year_months: dict[str, MonthDatasetStatus] = Field(default_factory=dict)
    changed: bool = Field(False, exclude=True)

    @staticmethod
    def load_status() -> "MainStatus":
        if not MAIN_STATUS_FILE_PATH.exists():
            return MainStatus()
        else:
            return MainStatus.model_validate_json(MAIN_STATUS_FILE_PATH.open(encoding="utf-8").read())

    def store_status(self):
        json.dump(self.model_dump(), MAIN_STATUS_FILE_PATH.open("w", encoding="utf-8"), indent=2)

    # months
    def sync_months(self):
        folder_content = CONFIG.STREAM_BASE_FOLDER.glob("*")
        for folder in folder_content:
            if folder.name.startswith("archiveteam-twitter-stream-"):
                datum_parts = '-'.join(folder.name.split("-")[-2:])
                date = datetime.strptime(datum_parts, "%Y-%m")
                ym = YearMonth(date.year, date.month)
                if ym in self.year_months:
                    continue
                else:
                    sf = MonthDatasetStatus(key=ym, folder_name=folder.name)
                    self.year_months[str(ym)] = sf

    def print_database_status(self, month_status: MonthDatasetStatus):
        stats_fp = month_status.stats_file_path
        if not stats_fp.exists():
            logger.warning(f"{month_status.key}: stats file missing")


Main_Status: MainStatus = None
