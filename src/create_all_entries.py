import io
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional

import jsonlines
from sqlalchemy.orm import Session
from tqdm import tqdm

from src.consts import logger, CONFIG
from src.db import main_db_path, init_db, DBPost
from src.db_funcs import consider_drop_table
from src.post_filter import check_original_tweet
from src.util import get_dump_path, consider_deletion, iter_tar_files, tarfile_datestr, iter_jsonl_files_data, post_url


def create_main_db_entry(data: dict, location_index: list[str]) -> DBPost:
    # previous one, that we need differently later
    # from TimeRangeEvalEntry
    post_dt = datetime.fromtimestamp(int(int(data['timestamp_ms']) / 1000))
    post = DBPost(
        platform="twitter",
        post_url_computed=post_url(data),
        date_created=post_dt,
        content=data if CONFIG.STORE_COMPLETE_CONTENT else None,
        text=data["text"],
        language=data["lang"],
        location_index=location_index,
    )
    post.set_date_columns()
    return post


def create_all_process_jsonl_entry(jsonl_entry: dict,
                        location_index: list[str]) -> Optional[DBPost]:
    if "data" in jsonl_entry:
        data = jsonl_entry["data"]
    else:  # 2022-01,02
        data = jsonl_entry

    if check_original_tweet(data) and data.get("lang") in CONFIG.LANGUAGES:
        db_post = create_main_db_entry(jsonl_entry, location_index)
        return db_post

    return None


def create_all_process_jsonl_file(jsonl_file_data: bytes,
                       location_index: list[str]) ->  list[DBPost]:
    entries_count = 0
    posts: list[DBPost] = []

    for jsonl_entry in jsonlines.Reader(io.BytesIO(jsonl_file_data)):
        location_index.append(entries_count)
        entries_count += 1
        # language and original tweet filter
        post: Optional[DBPost] = create_all_process_jsonl_entry(jsonl_entry, location_index.copy())
        if post:
            posts.append(post)
        location_index.pop()

    return  posts


def create_all_process_tar_file(tar_file: Path,
                         session: Session,
                         location_index: list[str]):
        # hour:language:post

        posts: list[DBPost] = []
        for jsonl_file_name, jsonl_file_data in tqdm(iter_jsonl_files_data(tar_file)):
            location_index.append(jsonl_file_name)
            # process jsonl file
            posts = create_all_process_jsonl_file(jsonl_file_data, location_index)
            location_index.pop()

            logger.debug(f"num posts: {len(posts)}")
            for post in posts:
                session.add(post)
            if len(session.new) > CONFIG.DUMP_THRESH:
                logger.debug(f"committing to db")
                session.commit()

        session.commit()


def create_all_process_dump(dump_path: Path, session: Session):
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
            create_all_process_tar_file(tar_file, session, location_index)
            location_index.pop()

    finally:
        session.close()

def main_create_all_data_db():
    try:
        month = 3
        year = 2028
        dump_path = get_dump_path(year,month)
        if not dump_path.exists():
            logger.error(f"dumppath {dump_path} does not exist")
            return
        db_path = main_db_path(year, month)
        session_maker = init_db(db_path)
        session = session_maker()
        # call process func
        create_all_process_dump(dump_path, session)
    except KeyboardInterrupt:

        consider_drop_table(session, DBPost)
        consider_deletion(db_path)
    except Exception as e:
        traceback.print_exc()
        consider_deletion(db_path)



if __name__ == "__main__":
    main_create_all_data_db()
