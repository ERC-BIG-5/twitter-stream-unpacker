import io
from abc import ABC, abstractmethod
from pathlib import Path
from typing import cast, Type, Any, Optional

import jsonlines
from tqdm import tqdm

from src.consts import logger, locationindex_type, CONFIG
from src.models import IterationSettings, ProcessCancel
from src.process_methods.abstract_method import IterationMethod
from src.status import MainStatus, MonthDatasetStatus
from src.util import get_dump_path, iter_tar_files, tarfile_datestr, iter_jsonl_files_data

def _generic_process_jsonl_entry(jsonl_entry: dict,
                                 location_index: locationindex_type,
                                 methods: list[IterationMethod]) -> None:
    if "data" in jsonl_entry:
        data = jsonl_entry["data"]
    else:  # 2022-01,02
        data = jsonl_entry

    for method in methods:
        res = method.process_data(data, tuple(location_index))
        if isinstance(res, ProcessCancel):
            logger.debug(res.reason)
            break
    return None


def _generic_process_jsonl_file(jsonl_file_data: bytes,
                                location_index: list[str],
                                methods: list[IterationMethod]):
    entries_count = 0

    for jsonl_entry in jsonlines.Reader(io.BytesIO(jsonl_file_data)):
        location_index.append(entries_count)
        entries_count += 1
        # language and original tweet filter
        _generic_process_jsonl_entry(jsonl_entry, cast(locationindex_type, location_index.copy()), methods)
        location_index.pop()


def _generic_process_tar_file(tar_file: Path,
                              location_index: list[str],
                              methods: list[IterationMethod]):
    test_count = 0
    for jsonl_file_name, jsonl_file_data in tqdm(iter_jsonl_files_data(tar_file)):
        location_index.append(jsonl_file_name)
        # process jsonl file
        _generic_process_jsonl_file(jsonl_file_data, location_index, methods)
        location_index.pop()
        test_count += 1

        if CONFIG.TEST_MODE and test_count == CONFIG.TEST_NUM_JSONL_FILES:
            break


def _generic_process_dump(dump_path: Path, methods: list[IterationMethod]):
    dump_file_date_name = dump_path.name.lstrip("archiveteam-twitter-stream")
    location_index: list[str] = [dump_file_date_name]
    logger.debug(f"dump: {dump_file_date_name}")
    # iter the tar files in the dump
    tar_files = list(iter_tar_files(dump_path))
    for idx, tar_file in enumerate(tar_files):
        tar_file_date_name = tarfile_datestr(tar_file)
        logger.info(f"tar file: {tar_file_date_name} - {idx} / {len(tar_files)}")
        location_index.append(tar_file_date_name)
        # process tar file
        _generic_process_tar_file(tar_file, location_index, methods)
        location_index.pop()
        if CONFIG.TEST_MODE and idx == CONFIG.TEST_NUM_TAR_FILES:
            break


def complex_main_generic_all_data(settings: IterationSettings,
                                  status: Optional[MonthDatasetStatus],
                                  methods: list[Type[IterationMethod]]):
    if CONFIG.TEST_MODE:
        logger.info("Test-mode on")
    dump_path = get_dump_path(settings.year, settings.month)
    if not dump_path.exists():
        logger.error(f"dumppath {dump_path} does not exist")
        return
    # call process func
    _methods = [method(settings) for method in methods]
    _method_dict = {method.name: method for method in _methods}
    for method in _methods:
        method.set_methods(_method_dict)

    _generic_process_dump(dump_path, _methods)

    for method in _methods:
        if status:
            method.set_ds_status_field(status)
        method.finalize()




