import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Optional, Any

import deepdiff
import genson
from sqlalchemy.orm import sessionmaker

from src.consts import locationindex_type, CONFIG, ANNOT_EXTRA_TEST_ROUND, BASE_STAT_PATH, METHOD_FILTER, METHOD_STATS, \
    METHOD_INDEX_DB, METHOD_ANNOTATION_DB, METHOD_SCHEMA
from src.db.db import init_db, main_db_path
from src.db.models import DBPostIndexPost
from src.models import AnnotPostCollection, ProcessCancel
from src.mutli_func_iter import IterationMethod, IterationSettings, complex_main_generic_all_data
from src.post_filter import is_original_tweet
from src.status import MonthDatasetStatus
from src.util import post_date, post_url, year_month_str


class PostFilterMethod(IterationMethod):
    """
    Filters posts that are in the selected languages and are original
    """

    @property
    def name(self) -> str:
        return METHOD_FILTER

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        if post_data.get("lang") in CONFIG.LANGUAGES and is_original_tweet(post_data):
            return post_data.get("lang")
        return ProcessCancel("filtered out")

    def finalize(self):
        pass


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

class EntrySchema(IterationMethod):

    @property
    def name(self) -> str:
        return METHOD_SCHEMA

    def __init__(self, settings: IterationSettings):
        super().__init__(settings)
        self.collect_num_posts = 50
        self.create_schema_from: list[dict] = []


    def _process_data(self, post_data: dict, location_index: locationindex_type):
        lang_or_none = self._methods[METHOD_FILTER].current_result
        self.create_schema_from.append(post_data)
        if len(self.create_schema_from) == self.collect_num_posts:
            self._build_schema()

    def _build_schema(objects: list[dict], check_div_every_k: Optional[int] = None) -> dict:
        """
        note: this seems to be the way to avoid that some props are removed.

        :param objects:
        :param check_div_every_k:
        :return:
        """
        builder = genson.SchemaBuilder()
        cur_schema = {}
        for idx, obj in enumerate(objects):
            cur_builder = genson.SchemaBuilder()
            cur_builder.add_object(obj)
            builder.add_schema(cur_builder)
            if idx == 0:
                cur_schema = builder.to_schema()
            if check_div_every_k is not None and (idx != 0 and idx % check_div_every_k == 0):
                next_schema = builder.to_schema()
                diff = deepdiff.DeepDiff(cur_schema, next_schema)
                print(json.dumps(json.loads(diff.to_json()), indent=2))
                # print(json.dumps(diff.to_dict(), indent=2))

        return cur_schema



class IndexEntriesDB(IterationMethod):
    """
    Create an index db entry, that allows to look up
    """

    @property
    def name(self) -> str:
        return METHOD_INDEX_DB

    def __init__(self, settings: IterationSettings):
        super().__init__(settings)

        self.index_entries: dict[str, list[DBPostIndexPost]] = {}
        self.DUMP_THRESH = 500
        self._language_sessionmakers: dict[str, sessionmaker] = {}
        for lang in settings.languages:
            self.index_entries[lang] = []
            self._language_sessionmakers[lang] = init_db(
                main_db_path(settings.year,
                             settings.month,
                             lang,
                             settings.annotation_extra))

    @staticmethod
    def _create_index_entry(post_data: dict, location_index: locationindex_type) -> DBPostIndexPost:
        post_dt = post_date(post_data['timestamp_ms'])
        post = DBPostIndexPost(
            platform="twitter",
            post_url_computed=post_url(post_data),
            date_created=post_dt,
            language=post_data["lang"],
            location_index=list(location_index),
        )
        post.set_date_columns()
        return post

    def _process_data(self, post_data: dict, location_index: locationindex_type):
        lang_or_none = self._methods[METHOD_FILTER].current_result
        if lang_or_none:

            entry = self._create_index_entry(post_data, location_index)
            lang = entry.language
            self.index_entries[lang].append(entry)

            if len(self.index_entries[lang]) > self.DUMP_THRESH:
                with self._language_sessionmakers[lang]() as session:
                    session.add_all(self.index_entries[lang])
                    session.commit()
                    self.index_entries[lang].clear()

    def finalize(self):
        for lang in self._language_sessionmakers:
            with self._language_sessionmakers[lang]() as session:
                session.add_all(self.index_entries[lang])
                session.commit()
                self.index_entries[lang].clear()


class AnnotationDBMethod(IterationMethod):

    def __init__(self, settings: IterationSettings):
        super().__init__(settings)

        self.post_collection = AnnotPostCollection(settings.languages,
                                                   settings.year,
                                                   settings.month,
                                                   settings.annotation_extra)

    @property
    def name(self) -> str:
        return METHOD_ANNOTATION_DB

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        lang_or_none = self._methods[METHOD_FILTER].current_result
        if lang_or_none:
            self.post_collection.add_post(post_data, location_index)

    def finalize(self):
        self.post_collection.validate()
        self.post_collection.finalize_dbs()


def iter_dumps_main(settings: IterationSettings, month_ds_status: Optional[MonthDatasetStatus]):
    complex_main_generic_all_data(settings, month_ds_status, [PostFilterMethod,
                                                              StatsCollectionMethod,
                                                              IndexEntriesDB,
                                                              AnnotationDBMethod])


if __name__ == "__main__":
    _settings = IterationSettings(2022, 1, CONFIG.LANGUAGES, ANNOT_EXTRA_TEST_ROUND)
    iter_dumps_main(_settings, None)
