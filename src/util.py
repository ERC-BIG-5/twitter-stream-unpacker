import bz2
import gzip
import io
import json
import tarfile
import zlib
from datetime import datetime
from pathlib import Path
from tarfile import ReadError
from typing import Generator, Union, Optional

from deprecated import deprecated
from jsonlines import jsonlines

from src.consts import logger, CONFIG


def get_base_dump_path(year: int, month: int) -> Path:
    return CONFIG.STREAM_BASE_FOLDER / f"archiveteam-twitter-stream-{year}-{str(month).rjust(2, '0')}"


def iter_tar_files(path: Path) -> Generator[Path, None, None]:
    for p in sorted(path.glob("twitter-stream-*.tar")):
        yield p


def list_jsonl_file(path: Path) -> Generator[Path, None, None]:
    return path.glob("*.jsonl")


def tarfile_datestr(tar_file: Path) -> str:
    return tar_file.name.lstrip("twitter-stream-").rstrip(".tar")


def read_gzip_file(path: Path) -> bytes:
    with gzip.GzipFile(path) as gz_bytes:
        return gz_bytes.read()

def iter_jsonl_files_data(tar_file: Path) -> Generator[tuple[str, bytes], None, None]:
    with tarfile.open(tar_file, 'r') as tar:
        try:
            relevant_members = [member for member in tar.getmembers() if
                                (member.name.endswith('.json.bz2') or member.name.endswith('.json.gz'))]
        except ReadError as err:
            logger.error(f"Error getting members of tar file: {tar_file}")
            logger.error(err)
            return []

        # sort them (their name includes the datetime)
        def sort_key(tar_info):
            return Path(tar_info.name).stem.split('.')[0]

        relevant_members = sorted(relevant_members, key=sort_key)

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
                try:
                    data = extracted_file.read()
                    with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz_bytes:
                        yield member.name, gz_bytes.read()
                except ReadError as err:
                    logger.error(f"Error reading {member.name}: {str(err)}")
                    return None
                except zlib.error as e:
                    logger.error(f"Error reading {member.name}: {str(e)}")
                    return None

def iter_jsonl_file(fp: Path) -> Generator[dict, None, None]:
    """
    iterate through a jsonl file, and run through dicts
    :param fp:
    :return:
    """
    with jsonlines.open(fp) as fin:
        for line in fin:
            yield line


def consider_deletion(path: Path):
    delete_resp = input(f"Do you want to delete the file"
                        f"{path}? : y/ other key\n")
    if delete_resp == "y":
        logger.info(f"deleting: {path}")
        path.unlink()


def post_url(data: dict) -> str:
    return f"https://x.com/{data['user']['screen_name']}/status/{data['id']}"


def post_date2(post_data: dict) -> datetime:
    return datetime.fromtimestamp(int(int(post_data['timestamp_ms']) / 1000))


@deprecated(reason="use post_date2")
def post_date(ts: int | str) -> datetime:
    return datetime.fromtimestamp(int(int(ts) / 1000))


def year_month_str(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def get_post_text(post_data: dict) -> str:
    if post_data["truncated"]:
        return post_data["extended_tweet"]["full_text"]
    else:
        return post_data["text"]


def get_hashtags(post_data: dict) -> list[str]:
    return [ht["text"] for ht in post_data.get("entities", {}).get("hashtags", [])]


def load_data(file_path: Path) -> dict:
    return json.load(file_path.open(encoding="utf-8"))


def write_data(data: Union[dict, list], file_path: Path, indent: Optional[int] = 2):
    json.dump(data, file_path.open("w",encoding="utf-8"), ensure_ascii=False, indent=indent)

def json_gz_stem(file: str) -> str:
    """
    turns '20220101/20220101000000.json.gz' into '20220101000000'
    """
    return Path(Path(file).stem).stem