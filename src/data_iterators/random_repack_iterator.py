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
            self.max_index = last_entry.year_group_index + last_entry.count
            # print(self.max_index)

    def __iter__(self):
        return self

    def get_file_and_index(self) -> tuple[str, int]:
        random_index = random.randint(0, self.max_index - 1)
        print(f"Random index: {random_index}")

        with self.session_maker() as session:
            # Find the year entry
            year_entry = session.execute(
                select(RepackStats)
                .where(RepackStats.language == self.settings.languages[0])
                .where(RepackStats.year_group_index <= random_index)
                .where(RepackStats.year_group_index + RepackStats.count > random_index)
                .order_by(RepackStats.year_group_index.desc())
                .limit(1)
            ).scalar_one_or_none()

            if not year_entry:
                return None, None  # or handle this case as appropriate

            year_offset = random_index - year_entry.year_group_index
            # print(f"Year entry: {year_entry.year}, offset: {year_offset}")

            # Find the month entry within the year
            month_entry = session.execute(
                select(RepackStats)
                .where(RepackStats.language == self.settings.languages[0])
                .where(RepackStats.year == year_entry.year)
                .where(RepackStats.month_group_index <= year_offset)
                .where(RepackStats.month_group_index + RepackStats.count > year_offset)
                .order_by(RepackStats.month_group_index.desc())
                .limit(1)
            ).scalar_one_or_none()

            if not month_entry:
                return year_entry.path, year_offset  # fallback to year entry

            month_offset = year_offset - month_entry.month_group_index
            # print(f"Month entry: {month_entry.month}, offset: {month_offset}")

            # Find the day entry within the month
            day_entry = session.execute(
                select(RepackStats)
                .where(RepackStats.language == self.settings.languages[0])
                .where(RepackStats.year == year_entry.year)
                .where(RepackStats.month == month_entry.month)
                .where(RepackStats.day_group_index <= month_offset)
                .where(RepackStats.day_group_index + RepackStats.count > month_offset)
                .order_by(RepackStats.day_group_index.desc())
                .limit(1)
            ).scalar_one_or_none()

            if not day_entry:
                return month_entry.path, month_offset  # fallback to month entry

            day_offset = month_offset - day_entry.day_group_index
            # print(f"Day entry: {day_entry.day}, offset: {day_offset}")

            # Find the hour entry within the day
            hour_entry = session.execute(
                select(RepackStats)
                .where(RepackStats.language == self.settings.languages[0])
                .where(RepackStats.year == year_entry.year)
                .where(RepackStats.month == month_entry.month)
                .where(RepackStats.day == day_entry.day)
                .where(RepackStats.hour_group_index <= day_offset)
                .where(RepackStats.hour_group_index + RepackStats.count > day_offset)
                .order_by(RepackStats.hour_group_index.desc())
                .limit(1)
            ).scalar_one_or_none()

            if not hour_entry:
                return day_entry.path, day_offset  # fallback to day entry

            final_offset = day_offset - hour_entry.hour_group_index
            # print(f"Hour entry: {hour_entry.hour}, offset: {final_offset}")

            return hour_entry.path, final_offset

    def __next__(self):
        rel_path, index = self.get_file_and_index()
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

        return rel_path, index, post_data

    def __del__(self):
        # Ensure the session is closed when the object is garbage collected
        if self.session:
            self.session.close()