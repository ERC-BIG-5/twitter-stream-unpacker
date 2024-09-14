import atexit
import bz2
import gzip
import io
import json
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Generator

import jsonlines
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from consts import CONFIG, STATUS_FILE, BASE_PROCESS_PATH, logger, COMPLETE_FLAG
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


def process_dump(dump_path: Path, session_maker: sessionmaker):
    dump_file_date = dump_path.name.lstrip("archiveteam-twitter-stream")
    dump_file_status = status.setdefault(dump_file_date, {})
    if dump_file_status.get(COMPLETE_FLAG):
        logger.debug(f"DUMP {dump_file_date} is already complete")
        return
    logger.debug(f"dumping: {dump_file_date}")
    dump_dir = BASE_PROCESS_PATH / dump_file_date
    dump_dir.mkdir(exist_ok=True)
    session = session_maker()
    try:
        # iter the tar files in the dump
        tar_files = list(dump_path.glob("twitter-stream-*.tar"))
        for tar_idx, tar_file in enumerate(tar_files):
            print(f"{tar_idx} / {len(tar_files)}")
            tar_date_name = tar_file.name.lstrip("twitter-stream-").rstrip(".tar")
            logger.info(f"tar_file: {tar_date_name}")
            tar_file_status = dump_file_status.setdefault(tar_date_name, {})
            if dump_file_status.get(tar_date_name, {}).get(COMPLETE_FLAG):
                logger.debug(f"tar file is already complete: {tar_date_name}")
                continue
            dump_file_status[tar_date_name] = tar_file_status
            # iter the jsonl files in the tar file
            for jsonl_file_name, jsonl_file_data in tqdm(iter_jsonl_files_data(tar_file)):
                entries_count = 0
                accepted = 0
                earliest_dt = None
                latest_dt = None
                json_file_status = {}
                tar_file_status[jsonl_file_name] = json_file_status
                # iter the jsonl entries in the jsonl file
                for jsonl_entry in (jsonlines.Reader(io.BytesIO(jsonl_file_data))):
                    entries_count += 1
                    if "data" in jsonl_entry:
                        data = jsonl_entry["data"]
                    else:  # 2022-01,02
                        data = jsonl_entry
                    if check_original_tweet(data) and data.get("lang") in CONFIG.LANGUAGES:
                        # print(data)
                        accepted += 1
                        # logger.debug(f"{accepted} / {entries_count}")
                        db_post = create_db_entry(jsonl_entry)
                        # setup earliest and latest
                        if not earliest_dt:
                            earliest_dt = db_post.date_created
                            latest_dt = db_post.date_created
                        else:
                            if db_post.date_created < earliest_dt:
                                earliest_dt = db_post.date_created
                            if db_post.date_created > latest_dt:
                                latest_dt = db_post.date_created
                        session.add(db_post)
                        if len(session.new) > CONFIG.DUMP_THRESH:
                            session.commit()
                json_file_status.update(
                    {"entries_count": entries_count,
                     "accepted": accepted
                     })
            tar_file_status[COMPLETE_FLAG] = True
    finally:
        session.close()
    dump_file_status[COMPLETE_FLAG] = True


def exit_handler():
    with STATUS_FILE.open("w") as status_file:
        json.dump(status, status_file, indent=2)


def load_status() -> dict:
    return json.load(open(STATUS_FILE)) if STATUS_FILE.exists() else {}


def main():
    atexit.register(exit_handler)
    BASE_PROCESS_PATH.mkdir(exist_ok=True)
    for dump in iter_dumps():
        print(dump)
        dump_file_date = dump.name.lstrip("archiveteam-twitter-stream")
        month_no = int(dump_file_date.split("-")[1])
        session_maker = init_db(main_db_path(month_no))
        process_dump(dump, session_maker)


if __name__ == "__main__":
    status: dict = load_status()
    main()
