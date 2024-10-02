import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Optional, Any

from src.consts import METHOD_STATS, locationindex_type, METHOD_FILTER, BASE_STAT_PATH
from src.models import IterationSettings
from src.mutli_func_iter import IterationMethod
from src.status import MonthDatasetStatus
from src.util import year_month_str


@dataclass
class CollectionStats:
    items: Optional[dict[str, Any]] = None
    total_posts: int = 0
    accepted_posts: Counter = field(default_factory=Counter)

    def to_dict(self):
        d = self.__dict__.copy()
        if self.items:
            d["items"] = {k: v.to_dict() for k, v in self.items.items()}
        else:
            del d["items"]
        return d


class StatsCollectionMethod(IterationMethod):
    """
    collect stats from data:
        - all posts
        - all from Filter accepted posts, grouped by languages
    these stats are collected on
    - jsonl files
    - tar files
    - a whole dump folder (a month)
    """

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        status.stats_file_available = True



    @property
    def name(self) -> str:
        return METHOD_STATS

    def __init__(self, settings: IterationSettings):
        super().__init__(settings)
        self.stats = CollectionStats(items={})

    def _process_data(self, post_data: dict, location_index: locationindex_type):
        lang_or_none = self._methods[METHOD_FILTER].current_result

        dump_path, tar_file, jsonl_file, index = location_index
        tar_file_stat = self.stats.items.setdefault(tar_file, CollectionStats(items={}))
        jsonl_stats = tar_file_stat.items.setdefault(jsonl_file, CollectionStats())

        jsonl_stats.total_posts += 1
        if lang_or_none:
            jsonl_stats.accepted_posts[lang_or_none] += 1

    def finalize(self):
        for tar_file, tar_file_stats in self.stats.items.items():
            for jsonl_file, jsonl_file_stats in tar_file_stats.items.items():
                tar_file_stats.total_posts += jsonl_file_stats.total_posts
                tar_file_stats.accepted_posts += jsonl_file_stats.accepted_posts
            self.stats.total_posts += tar_file_stats.total_posts
            self.stats.accepted_posts += tar_file_stats.accepted_posts

        # todo this should be derived from the global status file, or pass it there
        stats_file_path = BASE_STAT_PATH / f"{year_month_str(self.settings.year, self.settings.month)}.json"

        json.dump(self.stats.to_dict(), stats_file_path.open("w", encoding="utf-8"),
                  indent=2)
