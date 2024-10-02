import bz2
import gzip
import io
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Generator

from jsonlines import jsonlines

from src.consts import logger, CONFIG


def get_dump_path(year: int, month: int) -> Path:
    return CONFIG.STREAM_BASE_FOLDER / f"archiveteam-twitter-stream-{year}-{str(month).rjust(2, '0')}"


def iter_tar_files(path: Path) -> Generator[Path, None, None]:
    for p in sorted(path.glob("twitter-stream-*.tar")):
        yield p


def list_jsonl_file(path: Path) -> Generator[Path, None, None]:
    return path.glob("*.jsonl")


def tarfile_datestr(tar_file: Path) -> str:
    return tar_file.name.lstrip("twitter-stream-").rstrip(".tar")


def iter_jsonl_files_data(tar_file: Path) -> Generator[tuple[str, bytes], None, None]:
    with tarfile.open(tar_file, 'r') as tar:
        relevant_members = [member for member in tar.getmembers() if
                            (member.name.endswith('.json.bz2') or member.name.endswith('.json.gz'))]
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
                with gzip.GzipFile(fileobj=io.BytesIO(extracted_file.read())) as gz_bytes:
                    yield member.name, gz_bytes.read()


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


def post_date(ts: int | str) -> datetime:
    return datetime.fromtimestamp(int(int(ts) / 1000))


def year_month_str(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"
