import io
from pathlib import Path
from typing import Callable, cast, Optional

import jsonlines
from tqdm import tqdm

from src.consts import logger, locationindex_type, CONFIG
from src.util import get_dump_path, iter_tar_files, tarfile_datestr, iter_jsonl_files_data


def generic_process_jsonl_entry(jsonl_entry: dict,
                                location_index: locationindex_type,
                                generic_func: Callable[[dict, locationindex_type], None]) -> None:
    if "data" in jsonl_entry:
        data = jsonl_entry["data"]
    else:  # 2022-01,02
        data = jsonl_entry

    generic_func(data, location_index)
    return None


def generic_process_jsonl_file(jsonl_file_data: bytes,
                               location_index: list[str],
                               generic_func: Callable[[dict, tuple[str, str, str, int]], None]):
    entries_count = 0

    for jsonl_entry in jsonlines.Reader(io.BytesIO(jsonl_file_data)):
        location_index.append(entries_count)
        entries_count += 1
        # language and original tweet filter
        generic_process_jsonl_entry(jsonl_entry, cast(locationindex_type, location_index.copy()), generic_func)
        location_index.pop()


def generic_process_tar_file(tar_file: Path,
                             location_index: list[str],
                             generic_func: Callable[[dict, tuple[str, str, str, int]], None]):
    for jsonl_file_name, jsonl_file_data in tqdm(iter_jsonl_files_data(tar_file)):
        location_index.append(jsonl_file_name)
        # process jsonl file
        generic_process_jsonl_file(jsonl_file_data, location_index, generic_func)
        location_index.pop()


def generic_process_dump(dump_path: Path, generic_func: Callable[[dict, tuple[str, str, str, int]], None]):
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
        generic_process_tar_file(tar_file, location_index, generic_func)
        location_index.pop()
        if CONFIG.TESTMODE:
            break


def main_generic_all_data(generic_func: Callable[[dict, Optional[locationindex_type]], None]):
    month = 1
    year = 2022
    dump_path = get_dump_path(year, month)
    if not dump_path.exists():
        logger.error(f"dumppath {dump_path} does not exist")
        return
    # call process func
    generic_process_dump(dump_path, generic_func)


