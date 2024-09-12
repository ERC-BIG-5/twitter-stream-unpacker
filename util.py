import json
from pathlib import Path
from typing import Generator

from jsonlines import jsonlines


def list_jsonl_file(path: Path) -> Generator[Path, None, None]:
    return path.glob("*.jsonl")


def load_jsonl_file(fp: Path) -> Generator[dict, None, None]:
    """
    iterate through a jsonl file, and run through dicts
    :param fp:
    :return:
    """
    with jsonlines.open(fp) as fin:
        for line in fin:
            yield line

