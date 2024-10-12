import gzip
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from jsonlines import jsonlines
from pydantic import BaseModel, ConfigDict

from src.consts import locationindex_type, BASE_REPACK_PATH, get_logger, DATA_SOURCE_DUMP
from src.models import IterationSettings, ProcessSkipType
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import year_month_str, post_date2

logger = get_logger(__file__)


class PackEntriesConfig(BaseModel):
    time_group_resolution: int = 15  # in minutes
    delete_jsonl_files: bool = True
    gzip_files: bool = True
    skip_existing_days: bool = True

    model_config = ConfigDict(extra='ignore')


@dataclass
class WriteInfo:
    bucket_dt: datetime
    file_path: Path
    writer: jsonlines.Writer


class PackEntries(IterationMethod):
    """
    Filters posts that are in the selected languages and are original
    """

    def __init__(self, settings: IterationSettings, config: dict) -> None:
        super().__init__(settings, config)
        self.config = PackEntriesConfig.model_validate(config or {})

        self.base_path = BASE_REPACK_PATH / year_month_str(settings.year, settings.month)
        self.base_path.mkdir(exist_ok=True)
        self.fouts: dict[str, WriteInfo] = {}
        self.skip_day: bool = self.config.skip_existing_days

    @staticmethod
    def compatible_with_data_sources() -> list[str]:
        return [DATA_SOURCE_DUMP]

    @staticmethod
    def name() -> str:
        return "repack"

    def zip_file(self, fp: Path):
        dest_fp = fp.parent / f"{fp.name}.gz"
        with open(fp, 'rt', encoding='utf-8') as f_in:
            with gzip.open(dest_fp, 'wb') as f_out:
                f_out.write(f_in.read().encode('utf-8'))

    def _finalize_file(self, info: WriteInfo):
        """
        close the writer, zip the file and delete the original
        Might just delete the file and not zip it, if there is no data
        Check config options, for deletion and zipping
        """
        info.writer.close()
        # check if we can just delete the file
        if self.config.gzip_files:
            self.zip_file(info.file_path)
        if self.config.delete_jsonl_files:
            info.file_path.unlink()

    def finalize_files(self):
        """
        finalize all files
        """
        for info in self.fouts.values():
            self._finalize_file(info)

    def _init_file_out(self, post_data: dict):
        post_date_ = post_date2(post_data)
        y_m_d_str = post_date_.strftime("%Y%m%d")
        day_folder = self.base_path / y_m_d_str
        day_lang_folder = day_folder / post_data["lang"]
        day_lang_folder.mkdir(exist_ok=True, parents=True)
        # get proper minute based on 'time_group_resolution'
        post_minute_of_day = post_date_.hour * 60 + post_date_.minute
        group_day_minute = int(
            post_minute_of_day / self.config.time_group_resolution) * self.config.time_group_resolution
        group_hour = int(group_day_minute / 60)
        group_minute = group_day_minute % 60
        group_hour_str = str(int(group_day_minute / 60)).rjust(2,'0')
        group_minute_str = str(group_day_minute % 60).rjust(2,'0')
        bucket_dt = datetime(post_date_.year, post_date_.month, post_date_.day, group_hour, group_minute)

        time_str = f"{y_m_d_str}{group_hour_str}{group_minute_str}"
        jsonl_file = day_lang_folder / f"{time_str}.jsonl"
        writer = jsonlines.open(jsonl_file, mode='w')

        self.fouts[post_data["lang"]] = WriteInfo(bucket_dt, jsonl_file, writer)

    def _check_day_exists(self, y_m_d_str: str) -> bool:
        return (self.base_path / y_m_d_str).exists()

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        if self.skip_day:
            if self._check_day_exists(location_index[1]):
                return ProcessSkipType.TAR_FILE
            else:
                self.skip_day = False
        lang = post_data["lang"]
        if not self.fouts.get(lang):
            self._init_file_out(post_data)
            logger.debug(f"NEW: post:{post_date2(post_data)}, bucket: None")
        else:
            post_date_ =post_date2(post_data)
            info = self.fouts[lang]
            # todo actually, check with last buckt...
            time_diff = post_date_ - info.bucket_dt
            # potentially finalize and initialize a new file
            if (time_diff.seconds / 60) >= self.config.time_group_resolution:
                logger.debug(f"post:{post_date_}, bucket: {info.bucket_dt}")
                self._finalize_file(info)
                self._init_file_out(post_data)

        self.fouts[lang].writer.write(post_data)

    def finalize(self):
        self.finalize_files()

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        pass
