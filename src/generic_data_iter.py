import io
import json
import sys
from pathlib import Path
from random import random
from typing import Optional, Callable

import jsonlines
from tqdm import tqdm

from src.consts import logger
from src.db import DBPost
from src.json_schema_builder import build_schema
from src.util import get_dump_path, iter_tar_files, tarfile_datestr, iter_jsonl_files_data


def generic_process_jsonl_entry(jsonl_entry: dict,
                                location_index: list[str],
                                generic_func: Callable[[dict], None]) -> None:
    if "data" in jsonl_entry:
        data = jsonl_entry["data"]
    else:  # 2022-01,02
        data = jsonl_entry

    generic_func(data)
    return None


def generic_process_jsonl_file(jsonl_file_data: bytes,
                               location_index: list[str], generic_func: Callable[[dict], None]):
    entries_count = 0

    for jsonl_entry in jsonlines.Reader(io.BytesIO(jsonl_file_data)):
        location_index.append(entries_count)
        entries_count += 1
        # language and original tweet filter
        generic_process_jsonl_entry(jsonl_entry, location_index.copy(), generic_func)
        location_index.pop()


def generic_process_tar_file(tar_file: Path,
                             location_index: list[str], generic_func: Callable[[dict], None]):
    for jsonl_file_name, jsonl_file_data in tqdm(iter_jsonl_files_data(tar_file)):
        location_index.append(jsonl_file_name)
        # process jsonl file
        generic_process_jsonl_file(jsonl_file_data, location_index, generic_func)
        location_index.pop()


def generic_process_dump(dump_path: Path, generic_func: Callable[[dict], None]):
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


def main_generic_all_data(generic_func: Callable[[dict], None]):
    month = 3
    year = 2028
    dump_path = get_dump_path(year, month)
    if not dump_path.exists():
        logger.error(f"dumppath {dump_path} does not exist")
        return
    # call process func
    generic_process_dump(dump_path, generic_func)


def generic_iter_collect_schema():
    """
    collect the schema over 100 tweets. semi-randomly selected
    :return:
    """
    def generate_json_schema():

        pick = []
        def pick_for_schema(data):
            if random() < 0.00005:
                pick.append(data)
                print(len(pick))
            if len(pick) == 100:
                schema = build_schema(pick)
                print(json.dumps(schema, indent=2))
                sys.exit()

        return pick_for_schema

    main_generic_all_data(generate_json_schema())


def collect_languages():

    languages: set[str] = set()

    def collect_lang(data: dict):
        languages.add(data["lang"])


    main_generic_all_data(collect_lang)
    print(languages)

if __name__ == "__main__":
    collect_languages()
