"""
This is the iterator for the raw dump files . not filtered and in the format

- dump per month
- tar file per day
- jsonl.gz for each minute

"""
import io
from pathlib import Path
from typing import cast, Optional

import jsonlines
from tqdm import tqdm

from src.consts import locationindex_type, CONFIG, get_logger
from src.models import IterationSettings, ProcessCancel, ProcessSkipType
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import get_base_dump_path, iter_tar_files, tarfile_datestr, iter_jsonl_files_data

logger = get_logger(__file__, "INFO")


def _base_jsonl_line_processor(jsonl_entry: dict,
                               location_index: locationindex_type,
                               methods: list[IterationMethod]) -> Optional[ProcessSkipType]:
    if "data" in jsonl_entry:
        data = jsonl_entry["data"]
    else:  # 2022-01,02
        data = jsonl_entry

    for method in methods:
        res = method.process_data(data, tuple(location_index))
        if isinstance(res, ProcessCancel):
            logger.debug(res.reason)
            break
        if isinstance(res, ProcessSkipType):
            return res
    return None


def _base_jsonl_file_iterator(jsonl_file_data: bytes,
                              location_index: list[str],
                              methods: list[IterationMethod]) -> Optional[ProcessSkipType]:
    entries_count = 0

    for jsonl_entry in jsonlines.Reader(io.BytesIO(jsonl_file_data)):
        location_index.append(entries_count)
        entries_count += 1
        # language and original tweet filter
        potential_skip = _base_jsonl_line_processor(jsonl_entry, cast(locationindex_type, location_index.copy()),
                                                    methods)
        location_index.pop()
        if potential_skip:
            # only
            if not potential_skip.JSON_FILE:
                return potential_skip
            break


def _base_tar_file_iterator(tar_file: Path,
                            location_index: list[str],
                            methods: list[IterationMethod]) -> Optional[ProcessSkipType]:
    test_count = 0
    # for jsonl_file_name, jsonl_file_data in iter_jsonl_files_data(tar_file):
    for jsonl_file_name, jsonl_file_data in tqdm(iter_jsonl_files_data(tar_file)):
        location_index.append(jsonl_file_name)
        # process jsonl file
        potential_skip = _base_jsonl_file_iterator(jsonl_file_data, location_index, methods)
        location_index.pop()
        test_count += 1
        if potential_skip:
            return potential_skip

        if CONFIG.TEST_MODE and test_count == CONFIG.TEST_NUM_JSONL_FILES:
            break


def _base_dump_iterator(dump_path: Path, methods: list[IterationMethod]):
    dump_file_date_name = dump_path.name.lstrip("archiveteam-twitter-stream")
    location_index: list[str] = [dump_file_date_name]
    logger.debug(f"dump: {dump_file_date_name}")
    # iter the tar files in the dump
    tar_files = list(iter_tar_files(dump_path))
    if CONFIG.TEST_MODE:
        logger.info(f"Test mode only takes {CONFIG.TEST_NUM_TAR_FILES} tar file(s)")
        tar_files = tar_files[:CONFIG.TEST_NUM_TAR_FILES]
    for idx, tar_file in enumerate(tar_files):
        tar_file_date_name = tarfile_datestr(tar_file)
        logger.info(f"tar file: {tar_file_date_name} - {idx + 1} / {len(tar_files)}")
        location_index.append(tar_file_date_name)
        # process tar file
        potential_skip = _base_tar_file_iterator(tar_file, location_index, methods)
        if potential_skip:
            return potential_skip
        location_index.pop()


def base_month_data_iterator(settings: IterationSettings,
                             status: Optional[MonthDatasetStatus],
                             methods: list[IterationMethod]):
    dump_path = get_base_dump_path(settings.year, settings.month)
    if not dump_path.exists():
        logger.error(f"dumppath {dump_path} does not exist")
        return
    # call process func
    _base_dump_iterator(dump_path, methods)

    for method in methods:
        if status:
            method.set_ds_status_field(status)
        method.finalize()
