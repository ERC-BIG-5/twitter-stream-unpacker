import datetime
import json
import random
import sys
from time import sleep
from typing import Optional

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from src.consts import BASE_STAT_PATH, BASE_REPACK_PATH, CONFIG, BASE_DATA_PATH
from src.data_iterators.base_iterator import BaseIterator
from src.helper.iter_collection_download_media import create_new_media_group
from src.helper.repack_stats import RepackStats
from src.models import IterationSettings, ProcessCancel
from src.post_filter import remove_user, download_media
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import iter_jsonl_data2


class RandomPackedDataIterator(BaseIterator):

    def __init__(self, settings: IterationSettings,
                 status: Optional[MonthDatasetStatus],
                 methods: list[IterationMethod]):
        super().__init__(settings, status, methods)
        # keep a set of all visited indiced, in order to avoid duplicates
        self.visited_indices: set[int] = set()

        repack_db = BASE_STAT_PATH / "repack_stats.db"
        if not repack_db.exists():
            print("no repack stats db found")
            sys.exit(1)

        engine = create_engine(f'sqlite:///{repack_db}')
        self.session_maker = sessionmaker(engine)
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
        # pick a random, not visited index
        trials = 10
        random_index = None
        while trials > 0:
            random_index = random.randint(0, self.max_index - 1)
            if random_index not in self.visited_indices:
                self.visited_indices.add(random_index)
                break
        if not random_index:
            print("failed to pick a new index... ending")
            return

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

    def __next__(self) -> tuple[str, int, dict, int]:
        find = self.get_file_and_index()
        if not find:
            print("strange, no entry...")
            return None
        rel_path, index, random_index = find
        fp = BASE_REPACK_PATH / rel_path
        # file_data = read_gzip_file(fp)
        post_data: dict = None
        all_lines = []
        for idx, json_line in enumerate(iter_jsonl_data2(fp)):
            if idx == index - 1:
                post_data = json.loads(json_line)
                break
            # all_lines.append(json_line)

        for method in self.methods:
            if not post_data:
                post_data = json.loads(all_lines[-1])
            res = method.process_data(post_data, None)
            if isinstance(res, ProcessCancel):
                return None

        return rel_path, index, post_data, random_index

    def __del__(self):
        # Ensure the session is closed when the object is garbage collected
        if self.session:
            self.session.close()

    def run(self):
        entries = []
        limit = CONFIG.COLLECTION_LIMIT
        num_reached = 0
        dt_str = datetime.datetime.now().strftime("%Y%m%d%H")
        folder = create_new_media_group(dt_str)
        # media_map: dict[int, list[str]] = {
        media_no_retrievable: list[int] = []
        for idx, a in tqdm(enumerate(self)):
            # print(a)
            if a:
                post = a[2]
                remove_user(post)
                # media_map[post["id"]] = get_media(post)
                byte_array = download_media(post)
                has_img = False
                for idx, type_img in enumerate(byte_array):
                    if not type_img:
                        continue
                    file_type, bytes = type_img
                    img_dest = folder / f'{post["id"]}_{idx}.{file_type}'
                    img_dest.write_bytes(bytes)
                    has_img = True
                if has_img:
                    entries.append(a)
                else:
                    media_no_retrievable.append(post["id"])
            if len(entries) % (limit / 20) == 0 and len(entries) > num_reached:
                print(len(entries))
                num_reached = len(entries)
            if len(entries) == limit:
                break
            sleep(random.randint(1, 5))
        dest_path = BASE_DATA_PATH / f"time_storage/{dt_str}.json"
        dest_path.parent.mkdir(exist_ok=True)
        dest_path.write_text(json.dumps(entries, ensure_ascii=False), encoding='utf-8')
        (BASE_DATA_PATH / f"no_media.json").write_text(json.dumps(media_no_retrievable, ensure_ascii=False),
                                                       encoding='utf-8')
