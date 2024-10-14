import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Optional, Any

from src.consts import METHOD_STATS, locationindex_type, METHOD_FILTER, BASE_STAT_PATH, logger
from src.models import IterationSettings
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import year_month_str, get_hashtags


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

    def __init__(self, settings: IterationSettings, config: dict):
        super().__init__(settings, config)
        self.stats = CollectionStats(items={})
        self.collect_hashtags: bool = self.config.get("collect_hashtags", False)
        if self.collect_hashtags:
            logger.info("collecting hashtags")
        # {lang: count[hashtag]}
        self.hashtags: dict[str, Counter[str]] = {
            lang: Counter() for lang in settings.languages
        }

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        status.stats_file_available = True

    @staticmethod
    def name() -> str:
        return METHOD_STATS

    def _process_data(self, post_data: dict, location_index: locationindex_type):

        dump_path, tar_file, jsonl_file, index = location_index
        tar_file_stat = self.stats.items.setdefault(tar_file, CollectionStats(items={}))
        jsonl_stats = tar_file_stat.items.setdefault(jsonl_file, CollectionStats())

        # TODO, FILTERED OUT ARE NOT COUNTED ANYMORE
        jsonl_stats.total_posts += 1
        jsonl_stats.accepted_posts[post_data["lang"]] += 1
        if self.collect_hashtags:
            self.hashtags[post_data["lang"]].update(get_hashtags(post_data))

    def finalize(self):
        for tar_file, tar_file_stats in self.stats.items.items():
            for jsonl_file, jsonl_file_stats in tar_file_stats.items.items():
                tar_file_stats.total_posts += jsonl_file_stats.total_posts
                tar_file_stats.accepted_posts += jsonl_file_stats.accepted_posts
            self.stats.total_posts += tar_file_stats.total_posts
            self.stats.accepted_posts += tar_file_stats.accepted_posts

        # todo this should be derived from the global status file, or pass it there
        stats_file_path = BASE_STAT_PATH / f"{year_month_str(self.settings.year, self.settings.month)}.json"
        hashtags_file_path = BASE_STAT_PATH / f"hashtags_{year_month_str(self.settings.year, self.settings.month)}.json"

        json.dump(self.stats.to_dict(), stats_file_path.open("w", encoding="utf-8"),
                  indent=2, ensure_ascii=False)
        if self.collect_hashtags:
            json.dump(self.hashtags, hashtags_file_path.open("w", encoding="utf-8"),
                      indent=2, ensure_ascii=False)
