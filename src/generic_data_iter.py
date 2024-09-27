import calendar
import io
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from random import random
from typing import Callable, Optional, cast

import jsonlines
from sqlalchemy.orm import Session
from tqdm import tqdm

from src.consts import logger
from deprecated.create_anon_entries import create_annot1
from src.db.db import init_db, annotation_db_path
from src.db.models import DBAnnot1Post
from src.json_schema_builder import build_schema
from src.post_filter import check_original_tweet
from src.util import get_dump_path, iter_tar_files, tarfile_datestr, iter_jsonl_files_data, post_date

TESTMODE = False

locationindex_type = tuple[str,str, str, int]

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
        generic_process_jsonl_entry(jsonl_entry, cast(locationindex_type,location_index.copy()), generic_func)
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
        if TESTMODE:
            break


def main_generic_all_data(generic_func: Callable[[dict, tuple[str, str, str, int]], None]):
    month = 1
    year = 2022
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


@dataclass
class AnnotCollectionEntry:
    dt: datetime
    post_data: dict
    location_index: tuple[str, str, str, int]


class AnnotPostCollection:

    def __init__(self, languages: set[str], year: int, month: int):
        self._col: dict[str, dict[int, dict[int, Optional[AnnotCollectionEntry]]]] = {}
        self._language_sessions: dict[str, Session] = {}

        for lang in languages:
            self._col[lang] = {}
            num_days = calendar.monthrange(year, month)[1]
            for day in range(1, num_days + 1):
                self._col[lang][day] = {i: None
                                        for i in range(0, 24)
                                        }
            self._language_sessions[lang] = init_db(annotation_db_path(year,
                                                                       month,
                                                                       lang,
                                                                       "1x"),
                                                    tables=[DBAnnot1Post])()

    def add_post(self, post_data: dict, location_index: tuple[str, str, str, int]):
        post_date_ = post_date(post_data['timestamp_ms'])
        day = post_date_.day
        hour = post_date_.hour
        post_lang = post_data['lang']
        current = self._col[post_lang][day][hour]
        if not current:
            logger.debug(f"set {day}.{hour}")
            self._col[post_lang][day][hour] = AnnotCollectionEntry(post_date_, post_data, location_index)
        else:
            if post_date_ < current.dt:
                self._col[post_lang][day][hour] = AnnotCollectionEntry(post_date_, post_data, location_index)

    def validate(self):
        for lang, days in self._col.items():
            for day, hours in days.items():
                for hour, col_entry in hours.items():
                    if not col_entry:
                        print(f"Missing post for: {lang}-{day}-{hour}")

    def finalize_dbs(self):
        for lang, days in self._col.items():
            session = self._language_sessions[lang]
            for day, hours in days.items():
                for hour, col_entry in hours.items():
                    if col_entry:
                        session.add(create_annot1(col_entry.post_data, col_entry.location_index))

            session.commit()
            session.close()


def create_anont_dbs(year: int, month: int, languages: set[str]):
    if TESTMODE:
        logger.info("Running TEST-MODE, aborting after one tar file")
    post_collection = AnnotPostCollection(languages, year, month)

    def insert_post(post_data: dict, location_index: tuple[str, str, str, int]):
        if not check_original_tweet(post_data):
            return
        # post = create_annot1(data)
        if post_data["lang"] not in languages:
            return
        post_collection.add_post(post_data, location_index)

    main_generic_all_data(insert_post)
    post_collection.validate()
    post_collection.finalize_dbs()

    """

    import subprocess
    subprocess.run(["shutdown", "-h", "now"])
    """


if __name__ == "__main__":
    #TESTMODE = True
    create_anont_dbs(2022, 1, {"en", "es"})
    # collect_languages()
