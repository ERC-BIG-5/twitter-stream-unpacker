import io
from pathlib import Path
from typing import Optional

from jsonlines import jsonlines

from src.consts import BASE_REPACK_PATH, get_logger
from src.models import IterationSettings, ProcessCancel
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import year_month_str, read_gzip_file

logger = get_logger(__file__, "INFO")

class RepackedDataIterator:

    def __init__(self,settings: IterationSettings,
                    status: Optional[MonthDatasetStatus],
                    methods: list[IterationMethod]):
        self.settings = settings
        self.status = status
        self.methods = methods

        ym_str = year_month_str(settings.year, settings.month)
        self.base_month_path = BASE_REPACK_PATH / ym_str
        if not self.base_month_path.exists():
            logger.error(f"repack month does not exist: {ym_str}")
            return

    def _repack_jsonl_line_processor(self, jsonl_entry: dict) -> None:
        if "data" in jsonl_entry:
            data = jsonl_entry["data"]
        else:  # 2022-01,02
            data = jsonl_entry

        for method in self.methods:
            res = method.process_data(data, None)
            if isinstance(res, ProcessCancel):
                logger.debug(res.reason)
                break
        return None

    def _repack_file_iterator(self, jsonl_file_data:bytes):
        for jsonl_entry in jsonlines.Reader(io.BytesIO(jsonl_file_data)):
            # language and original tweet filter
            self._repack_jsonl_line_processor(jsonl_entry)

    def _repack_day_iterator(self, day_path: Path):
        for lang in self.settings.languages:
            lang_path: Path = day_path / lang
            lang_day_files = sorted(lang_path.glob("*"))
            for file in lang_day_files:
                self._repack_file_iterator( read_gzip_file(file))


    def repack_month_iterator(self):
        days_dirs = sorted(self.base_month_path.glob("*"))
        for days_dir in days_dirs:
            self._repack_day_iterator(days_dir)


def repack_iterator(settings: IterationSettings,
                    status: Optional[MonthDatasetStatus],
                    methods: list[IterationMethod]):

    d_iterator = RepackedDataIterator(settings, status, methods)
    d_iterator.repack_month_iterator()

    # for method in methods:
    #     if status:
    #         method.set_ds_status_field(status)
    #     method.finalize()