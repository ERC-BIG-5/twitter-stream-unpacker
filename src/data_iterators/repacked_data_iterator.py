import io
from pathlib import Path
from typing import Optional

from jsonlines import jsonlines
from tqdm import tqdm

from src.consts import BASE_REPACK_PATH, get_logger, locationindex_type
from src.models import IterationSettings, ProcessCancel
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import year_month_str, read_gzip_file

logger = get_logger(__file__, "INFO")


class RepackedDataIterator:

    def __init__(self, settings: IterationSettings,
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

    def _repack_jsonl_line_processor(self, jsonl_entry: dict, location_index: list[str]) -> None:
        if "data" in jsonl_entry:
            data = jsonl_entry["data"]
        else:  # 2022-01,02
            data = jsonl_entry

        for method in self.methods:
            res = method.process_data(data, locationindex_type(location_index))
            if isinstance(res, ProcessCancel):
                logger.debug(res.reason)
                break
        return None

    def _repack_file_iterator(self, jsonl_file_data: bytes, location_index: list[str]):
        for idx, jsonl_entry in enumerate(jsonlines.Reader(io.BytesIO(jsonl_file_data))):
            # language and original tweet filter
            location_index.append(str(idx))
            self._repack_jsonl_line_processor(jsonl_entry, location_index)
            location_index.pop()

    def _repack_day_iterator(self, day_path: Path, location_index: list[str]):
        for lang in self.settings.languages:
            lang_path: Path = day_path / lang
            lang_day_files = sorted(lang_path.glob("*"))
            for file in tqdm(lang_day_files):
                location_index.extend([lang_path.parent.name, lang])
                self._repack_file_iterator(read_gzip_file(file), location_index)
                location_index.pop()
                location_index.pop()

    def repack_month_iterator(self):
        days_dirs = sorted(self.base_month_path.glob("*"))
        location_index: list[str] = [f"repack-{year_month_str(self.settings.year, self.settings.month)}"]
        for idx, days_dir in enumerate(days_dirs):
            print(f"{idx + 1} / {len(days_dirs)}")
            self._repack_day_iterator(days_dir, location_index)


def repack_iterator(settings: IterationSettings,
                    status: Optional[MonthDatasetStatus],
                    methods: list[IterationMethod]):
    d_iterator = RepackedDataIterator(settings, status, methods)
    d_iterator.repack_month_iterator()

    # for method in methods:
    #     if status:
    #         method.set_ds_status_field(status)
    #     method.finalize()
