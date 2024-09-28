import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from src.consts import MAIN_STATUS_FILE_PATH, CONFIG


@dataclass(frozen=True)
class YearMonth:
    year: int
    month: int

    def __str__(self):
        return f"{self.year:04d}-{self.month:02d}"

class AnnotationDb(BaseModel):
    pass


class Sourcefolder(BaseModel):
    key: YearMonth
    folder_name: str
    valid: Optional[bool] = False
    annotation_dbs: list[AnnotationDb] = Field(default_factory=list)


class MainStatus(BaseModel):
    year_months: dict[str, Sourcefolder] = Field(default_factory=dict)
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
                    sf = Sourcefolder(key=ym, folder_name = folder.name)
                    self.year_months[str(ym)] = sf


Main_Status: MainStatus = None