import atexit
import bz2
import gzip
import io
import json
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Generator, Union, Any

import jsonlines
from sqlalchemy.orm import Session
from tqdm.auto import tqdm

from consts import CONFIG, STATUS_FILE, BASE_PROCESS_PATH, logger, COMPLETE_FLAG, ACCEPTED_POSTS, TOTAL_POSTS, \
    ITEM_COUNT, ITEMS_PROCESSED
from db import init_db, main_db_path, DBPost


def iter_dumps() -> list[Path]:
    return CONFIG.STREAM_BASE_FOLDER.glob("archiveteam-twitter-stream-*")


def iter_jsonl_files_data(tar_file: Path) -> Generator[tuple[str, bytes], None, None]:
    with tarfile.open(tar_file, 'r') as tar:
        relevant_members = [member for member in tar.getmembers() if
                            (member.name.endswith('.json.bz2') or member.name.endswith('.json.gz'))]
        for member in relevant_members:
            extracted_file = tar.extractfile(member)
            if member.name.endswith("bz2"):
                if extracted_file is not None:
                    try:
                        decompressed_data = bz2.decompress(extracted_file.read())
                        yield member.name, decompressed_data
                    except Exception as e:
                        print(f"Error processing {member.name}: {str(e)}")
            else:
                with gzip.GzipFile(fileobj=io.BytesIO(extracted_file.read())) as gz_bytes:
                    yield member.name, gz_bytes.read()


def create_db_entry(data: dict) -> DBPost:
    post_dt = datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S %z %Y')
    post = DBPost(
        platform="twitter",
        post_url_computed=f"https://x.com/x/status/{data['id']}",
        date_created=post_dt,
        content=data if CONFIG.STORE_COMPLETE_CONTENT else None,
        text=data["text"],
        language=data["lang"]
    )
    post.set_date_columns()
    return post


def check_original_tweet(data: dict) -> bool:
    return (data.get("referenced_tweets") is None and
            data.get("in_reply_to_status_id") is None and
            data.get("quoted_status_id") is None and
            data.get("retweeted_status") is None) or not CONFIG.ONLY_ORIG_TWEETS


def process_jsonl_entry(jsonl_entry: dict, session: Session) -> bool:
    if "data" in jsonl_entry:
        data = jsonl_entry["data"]
    else:  # 2022-01,02
        data = jsonl_entry

    if check_original_tweet(data) and data.get("lang") in CONFIG.LANGUAGES:
        db_post = create_db_entry(jsonl_entry)
        session.add(db_post)
        return True

    return False


def process_jsonl_file(jsonl_file_data: bytes, jsonl_file_name: str, session: Session, jsonl_file_status: dict) -> None:
    if jsonl_file_status.get(COMPLETE_FLAG):
        logger.debug(f"jsonl file is already complete: {jsonl_file_name}")
        return

    entries_count = 0
    accepted = 0

    for jsonl_entry in jsonlines.Reader(io.BytesIO(jsonl_file_data)):
        entries_count += 1
        if process_jsonl_entry(jsonl_entry, session):
            accepted += 1

    try:
        session.commit()
        jsonl_file_status.update({
            TOTAL_POSTS: entries_count,
            ACCEPTED_POSTS: accepted,
            COMPLETE_FLAG: True
        })
    except:
        session.rollback()
        raise

def process_tar_file(tar_file: Path,
                     session: Session,
                     tar_file_status: dict) -> None:
    if tar_file_status.get(COMPLETE_FLAG):
        logger.debug(f"tar file is already complete: {tar_file.name}")
        return

    tar_file_status[ITEMS_PROCESSED] = 0
    tar_file_status[TOTAL_POSTS] = 0
    tar_file_status[ACCEPTED_POSTS] = 0

    for jsonl_file_name, jsonl_file_data in tqdm(iter_jsonl_files_data(tar_file)):
        jsonl_file_status = tar_file_status["items"].setdefault(jsonl_file_name, {})
        process_jsonl_file(jsonl_file_data, jsonl_file_name, session, jsonl_file_status)
        tar_file_status[ITEMS_PROCESSED] += 1
        tar_file_status[TOTAL_POSTS] += jsonl_file_status[TOTAL_POSTS]
        tar_file_status[ACCEPTED_POSTS] += jsonl_file_status[ACCEPTED_POSTS]

    # set status
    tar_file_status[ITEM_COUNT] = tar_file_status[ITEMS_PROCESSED]
    del tar_file_status[ITEMS_PROCESSED]
    tar_file_status[COMPLETE_FLAG] = True


def process_dump(dump_path: Path, session: Session):
    dump_file_date_name = dump_path.name.lstrip("archiveteam-twitter-stream")
    dump_file_status: dict[str, Union[str, bool, int, dict[str, Any]]] = global_status.setdefault(dump_file_date_name,
                                                                                                  {"items": {}})
    if dump_file_status.get(COMPLETE_FLAG):
        logger.debug(f"dump {dump_file_date_name} is already complete")
        return

    logger.debug(f"dump: {dump_file_date_name}")

    dump_file_status[ITEMS_PROCESSED] =  0
    dump_file_status[TOTAL_POSTS] = 0
    dump_file_status[ACCEPTED_POSTS] = 0

    try:
        # iter the tar files in the dump
        tar_files = list(dump_path.glob("twitter-stream-*.tar"))
        for idx, tar_file in enumerate(tar_files):
            # process_tar_file
            tar_file_date_name = tar_file.name.lstrip("twitter-stream-").rstrip(".tar")
            tar_file_status = dump_file_status["items"].setdefault(tar_file_date_name, {"items": {}})
            logger.info(f"tar file: {tar_file_date_name} - {idx} / {len(tar_files)}")
            process_tar_file(tar_file, session, tar_file_status)

            dump_file_status[ITEMS_PROCESSED] += 1
            dump_file_status[TOTAL_POSTS] += tar_file_status[TOTAL_POSTS]
            dump_file_status[ACCEPTED_POSTS] += tar_file_status[ACCEPTED_POSTS]

        dump_file_status[ITEM_COUNT] = dump_file_status[ITEMS_PROCESSED]
        del dump_file_status[ITEMS_PROCESSED]
        dump_file_status[COMPLETE_FLAG] = True
    finally:
        session.close()


def exit_handler():
    with STATUS_FILE.open("w") as status_file:
        json.dump(global_status, status_file, indent=2)


def load_status() -> dict:
    return json.load(open(STATUS_FILE)) if STATUS_FILE.exists() else {}


def main():
    atexit.register(exit_handler)
    BASE_PROCESS_PATH.mkdir(exist_ok=True)
    for dump in iter_dumps():
        dump_file_date = dump.name.lstrip("archiveteam-twitter-stream")
        month_no = int(dump_file_date.split("-")[1])
        session_maker = init_db(main_db_path(month_no))
        session = session_maker()
        process_dump(dump, session)


if __name__ == "__main__":
    global_status: dict = load_status()
    main()
