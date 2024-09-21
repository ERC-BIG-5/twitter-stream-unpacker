from pathlib import Path

from consts import logger
from pathlib import Path
from typing import Generator

from jsonlines import jsonlines


def list_jsonl_file(path: Path) -> Generator[Path, None, None]:
    return path.glob("*.jsonl")


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