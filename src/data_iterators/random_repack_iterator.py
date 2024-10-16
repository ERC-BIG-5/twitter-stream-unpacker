import json
import random
import sys
from typing import Optional

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from src.consts import BASE_STAT_PATH, BASE_REPACK_PATH
from src.data_iterators.base_iterator import BaseIterator
from src.helper.repack_stats import RepackStats
from src.models import IterationSettings, ProcessCancel
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import read_gzip_file, iter_jsonl_data, iter_jsonl_data2


class RandomPackedDataIterator(BaseIterator):

    def __init__(self, settings: IterationSettings,
                 status: Optional[MonthDatasetStatus],
                 methods: list[IterationMethod]):
        super().__init__(settings, status, methods)

        repack_db =  BASE_STAT_PATH / "repack_stats.db"
        if not repack_db.exists():
            print("no repack stats db found")
            sys.exit(1)

        engine = create_engine(f'sqlite:///{repack_db}')
        self.session_maker= sessionmaker(engine)
        self.session = None
        with self.session_maker() as session:
            last_entry = session.execute(
                select(RepackStats).order_by(RepackStats.id.desc()).limit(1)
            ).scalar_one_or_none()
            self.max_index = last_entry.total_index + last_entry.count
            # print(self.max_index)

    def __iter__(self):
        return self

    def get_file_and_index(self) -> Optional[tuple[str, int, int]]:
        random_index = random.randint(0, self.max_index - 1)
        # print(f"Random index: {random_index}")
        # print(random_index)
        with self.session_maker() as session:
            entry = session.execute(
                select(RepackStats)
                .where(RepackStats.language == list(self.settings.languages)[0])
                .where(RepackStats.total_index <= random_index)
                .where(RepackStats.total_index + RepackStats.count > random_index)
            ).scalar()

            if entry:
                offset = random_index - entry.total_index
                return entry.path, offset, random_index

            return

    def __next__(self):
        find = self.get_file_and_index()
        if not find:
            print("strange, no entry...")
            return None
        rel_path, index,random_index = find
        fp = BASE_REPACK_PATH / rel_path
        # file_data = read_gzip_file(fp)
        post_data: dict = None
        all_lines = []
        for idx, json_line in enumerate(iter_jsonl_data2(fp)):
            if idx == index -1 :
                post_data = json.loads(json_line)
                break
            all_lines.append(json_line)

        for method in self.methods:
            if not post_data:
                post_data = json.loads(all_lines[-1])
            res = method.process_data(post_data, None)
            if isinstance(res, ProcessCancel):
                return None

        return rel_path, index, post_data,random_index

    def __del__(self):
        # Ensure the session is closed when the object is garbage collected
        if self.session:
            self.session.close()