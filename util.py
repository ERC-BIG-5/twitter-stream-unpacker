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




# db_path = main_db_path(3)
# session:Session = init_db(db_path)()
# statement = select(DBPost).where(DBPost.month_created == 3 and DBPost.date_created == 1)
# posts = session.execute(statement)
