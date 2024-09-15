import bz2
import gzip
import io
import json
import tarfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Generator, Any, Optional

import jsonlines
from sqlalchemy.orm import Session
from tqdm.auto import tqdm

from consts import CONFIG, logger, BASE_DATA_PATH
from db import init_db, main_db_path, DBPost


@dataclass
class CollectionStatus:
    items: Optional[dict[str, Any]] = None
    total_posts: int = 0
    accepted_posts: int = 0

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


def create_db_entry(data: dict, location_index:list[str]) -> DBPost:
    post_dt = datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S %z %Y')
    post = DBPost(
        platform="twitter",
        post_url_computed=f"https://x.com/x/status/{data['id']}",
        date_created=post_dt,
        content=data if CONFIG.STORE_COMPLETE_CONTENT else None,
        text=data["text"],
        language=data["lang"],
        location_index=location_index,
    )
    post.set_date_columns()
    return post


def check_original_tweet(data: dict) -> bool:
    return (data.get("referenced_tweets") is None and
            data.get("in_reply_to_status_id") is None and
            data.get("quoted_status_id") is None and
            data.get("retweeted_status") is None) or not CONFIG.ONLY_ORIG_TWEETS


def process_jsonl_entry(jsonl_entry: dict,
                        session: Session,
                        location_index:list[str]) -> bool:
    if "data" in jsonl_entry:
        data = jsonl_entry["data"]
    else:  # 2022-01,02
        data = jsonl_entry

    if check_original_tweet(data) and data.get("lang") in CONFIG.LANGUAGES:
        db_post = create_db_entry(jsonl_entry, location_index)
        session.add(db_post)
        return True

    return False


def process_jsonl_file(jsonl_file_data: bytes,
                       session: Session,
                       location_index: list[str]) -> CollectionStatus:

    entries_count = 0
    accepted = 0

    for jsonl_entry in jsonlines.Reader(io.BytesIO(jsonl_file_data)):
        location_index.append(entries_count)
        entries_count += 1

        if process_jsonl_entry(jsonl_entry, session, location_index):
            accepted += 1
        location_index.pop()
    return CollectionStatus(accepted_posts=accepted, total_posts=entries_count)

def process_tar_file(tar_file: Path,
                     session: Session,
                     location_index: list[str]) -> CollectionStatus:

    tar_file_status = CollectionStatus(items={})
    for jsonl_file_name, jsonl_file_data in tqdm(iter_jsonl_files_data(tar_file)):
        location_index.append(jsonl_file_name)
        jsonl_file_status = process_jsonl_file(jsonl_file_data, session, location_index)
        location_index.pop()
        tar_file_status.total_posts += jsonl_file_status.total_posts
        tar_file_status.accepted_posts += jsonl_file_status.accepted_posts
        tar_file_status.items[jsonl_file_name] = jsonl_file_status

        if len(session.new) > CONFIG.DUMP_THRESH:
            try:
                session.commit()
            except:
                session.rollback()
                raise
    return tar_file_status



def process_dump(dump_path: Path, session: Session) -> CollectionStatus:
    status = CollectionStatus(items={})
    dump_file_date_name = dump_path.name.lstrip("archiveteam-twitter-stream")
    location_index:list[str] = [dump_file_date_name]
    logger.debug(f"dump: {dump_file_date_name}")

    try:
        # iter the tar files in the dump
        tar_files = list(dump_path.glob("twitter-stream-*.tar"))
        for idx, tar_file in enumerate(tar_files):
            tar_file_date_name = tar_file.name.lstrip("twitter-stream-").rstrip(".tar")
            logger.info(f"tar file: {tar_file_date_name} - {idx} / {len(tar_files)}")
            location_index.append(tar_file_date_name)
            tar_file_results = process_tar_file(tar_file, session, location_index)
            location_index.pop()
            status.items += 1
            status.total_posts += tar_file_results.total_posts
            status += tar_file_results.accepted_posts
            status.items[tar_file_date_name] = tar_file_results

    finally:
        session.close()
    return status

def main():
    for dump in iter_dumps():
        start_time = datetime.now()
        dump_file_date = dump.name.lstrip("archiveteam-twitter-stream")
        month_no = int(dump_file_date.split("-")[1])
        session_maker = init_db(main_db_path(month_no))
        session = session_maker()
        status = process_dump(dump, session)
        json.dump(status, (BASE_DATA_PATH / f"{dump_file_date}.json").open("w"), indent=2)
        end_time = datetime.now()
        print(end_time - start_time)

if __name__ == "__main__":
    main()
