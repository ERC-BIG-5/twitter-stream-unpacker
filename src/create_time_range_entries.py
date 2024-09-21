import io
import io
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import jsonlines
from sqlalchemy.orm import Session
from tqdm.auto import tqdm

from consts import CONFIG, logger, BASE_STAT_PATH
from db import init_db, TimeRangeEvalEntry, main_db_path
from src.post_filter import check_original_tweet
from src.util import consider_deletion, get_dump_path, iter_tar_files, tarfile_datestr, iter_jsonl_files_data, post_url


@dataclass
class CollectionStatus:
    items: Optional[dict[str, Any]] = None
    total_posts: int = 0
    accepted_posts: int = 0

    def to_dict(self):
        d = self.__dict__.copy()
        if self.items:
            d["items"] = {k: v.to_dict() for k, v in self.items.items()}
        else:
            del d["items"]
        return d


def iter_dumps() -> list[Path]:
    return CONFIG.STREAM_BASE_FOLDER.glob("archiveteam-twitter-stream-*")


def create_time_range_entry(data: dict, location_index: list[str]) -> TimeRangeEvalEntry:
    post_dt = datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S %z %Y')
    post = TimeRangeEvalEntry(
        platform="twitter",
        post_url_computed=post_url(data),
        date_created=post_dt,
        language=data["lang"],
        location_index=location_index,
    )
    post.set_date_columns()
    return post


def process_jsonl_entry(jsonl_entry: dict,
                        location_index: list[str]) -> Optional[TimeRangeEvalEntry]:
    if "data" in jsonl_entry:
        data = jsonl_entry["data"]
    else:  # 2022-01,02
        data = jsonl_entry

    if check_original_tweet(data) and data.get("lang") in CONFIG.LANGUAGES:
        db_post = create_time_range_entry(jsonl_entry, location_index)
        return db_post

    return None


def process_jsonl_file(jsonl_file_data: bytes,
                       location_index: list[str]) -> tuple[
    CollectionStatus, list[TimeRangeEvalEntry]]:
    entries_count = 0
    accepted = 0

    posts: list[TimeRangeEvalEntry] = []

    for jsonl_entry in jsonlines.Reader(io.BytesIO(jsonl_file_data)):
        location_index.append(entries_count)
        entries_count += 1
        post: Optional[TimeRangeEvalEntry] = process_jsonl_entry(jsonl_entry, location_index.copy())
        # language and original tweet filter
        if post:
            posts.append(post)
        location_index.pop()
    accepted += len(posts)

    return CollectionStatus(accepted_posts=accepted, total_posts=entries_count), posts


def process_tar_file(tar_file: Path,
                     session: Session,
                     location_index: list[str]) -> CollectionStatus:
    tar_file_status = CollectionStatus(items={})
    posts: list[TimeRangeEvalEntry] = []
    for jsonl_file_name, jsonl_file_data in tqdm(iter_jsonl_files_data(tar_file)):
        location_index.append(jsonl_file_name)
        # process jsonl file
        jsonl_file_status, posts = process_jsonl_file(jsonl_file_data, location_index)
        location_index.pop()
        tar_file_status.total_posts += jsonl_file_status.total_posts
        tar_file_status.accepted_posts += jsonl_file_status.accepted_posts
        tar_file_status.items[jsonl_file_name] = jsonl_file_status

        logger.debug(f"num posts: {len(posts)}")
        for post in posts:
            session.add(post)
        # this we do in a separate step
        # for hour, lang_cols in hours_tweets.items():
        #     for lang, tweet in lang_cols.items():
        #         if not tweet:
        #             logger.warning(f"tar file: {tarfile_datestr(tar_file)} has "
        #                            f"a missing tweet: {hour},{lang}")
        #             continue
        #         session.add(tweet)
        if len(session.new) > CONFIG.DUMP_THRESH:
            logger.debug(f"committing to db")
            session.commit()

    session.commit()
    return tar_file_status


def process_dump(dump_path: Path, session: Session) -> CollectionStatus:
    status = CollectionStatus(items={})
    dump_file_date_name = dump_path.name.lstrip("archiveteam-twitter-stream")
    location_index: list[str] = [dump_file_date_name]
    logger.debug(f"dump: {dump_file_date_name}")
    try:
        # iter the tar files in the dump
        tar_files = list(iter_tar_files(dump_path))
        for idx, tar_file in enumerate(tar_files):
            tar_file_date_name = tarfile_datestr(tar_file)
            logger.info(f"tar file: {tar_file_date_name} - {idx} / {len(tar_files)}")
            location_index.append(tar_file_date_name)
            # process tar file
            tar_file_results = process_tar_file(tar_file, session, location_index)
            location_index.pop()
            status.total_posts += tar_file_results.total_posts
            status.accepted_posts += tar_file_results.accepted_posts
            status.items[tar_file_date_name] = tar_file_results

    finally:
        session.close()
    return status


def main_create_time_range_db():
    try:
        month = 3
        year = 2028
        dump_path = get_dump_path(year, month)
        if not dump_path.exists():
            logger.error(f"dumppath {dump_path} does not exist")
            return
        db_path = main_db_path(year, month)
        session_maker = init_db(db_path)
        session = session_maker()
        # call process func
        status: CollectionStatus = process_dump(dump_path, session)
        dump_file_date = dump_path.name.lstrip("archiveteam-twitter-stream")
        dump_file_name = f"{dump_file_date}.json"
        json.dump(status.to_dict(), (BASE_STAT_PATH / dump_file_name).open("w"), indent=2)
        logger.info(f"Created status file: {dump_file_name}")
    except KeyboardInterrupt:
        consider_deletion(db_path)
    except Exception as e:
        consider_deletion(db_path)


if __name__ == "__main__":
    main_create_time_range_db()
