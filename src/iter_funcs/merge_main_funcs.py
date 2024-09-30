import json
from abc import ABC
from collections import Counter
from dataclasses import dataclass, field
from typing import Optional, Any

from sqlalchemy.orm import sessionmaker

from src.consts import locationindex_type, CONFIG, ANNOT_EXTRA_TEST_ROUND
from src.db.db import init_db, main_db_path
from src.db.models import DBPostIndexPost
from src.mutli_func_iter import IterationMethod, IterationSettings, complex_main_generic_all_data
from src.post_filter import check_original_tweet
from src.util import post_date, post_url



class PostFilterMethod(IterationMethod):

    @property
    def name(self) -> str:
        return "filter"

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        if post_data.get("lang") in CONFIG.LANGUAGES and check_original_tweet(post_data):
            return post_data.get("lang")
        return None

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

    @property
    def name(self) -> str:
        return "stats"

    def __init__(self, settings: IterationSettings):
        super().__init__(settings)
        self.stats = CollectionStats(items={})

    def _process_data(self, post_data: dict, location_index: locationindex_type):
        lang_or_none = self._methods["filter"].current_result

        dump_path, tar_file, jsonl_file, index = location_index
        tar_file_stat = self.stats.items.setdefault(tar_file, CollectionStats(items={}))
        jsonl_stats = tar_file_stat.items.setdefault(jsonl_file, CollectionStats())

        jsonl_stats.total_posts += 1
        if lang_or_none:
            jsonl_stats.accepted_posts[lang_or_none] += 1
        # self.post_collection.add_post(post_data, location_index)

    def finalize(self):
        for tar_file, tar_file_stats in self.stats.items.items():
            print(tar_file)
            for jsonl_file, jsonl_file_stats in tar_file_stats.items.items():
                tar_file_stats.total_posts += jsonl_file_stats.total_posts
                tar_file_stats.accepted_posts += jsonl_file_stats.accepted_posts
            self.stats.total_posts += tar_file_stats.total_posts
            self.stats.accepted_posts += tar_file_stats.accepted_posts

        print(json.dumps(self.stats.to_dict(), indent=2))


class IndexEntriesDB(IterationMethod):

    @property
    def name(self) -> str:
        return "index"


    def __init__(self, settings: IterationSettings):
        super().__init__(settings)

        self.index_entries: dict[str, list[DBPostIndexPost]] = {}
        self.DUMP_THRESH = 500
        for lang in settings.languages:
            self.index_entries[lang] = []
            self._language_sessionmakers: dict[str, sessionmaker] = {
                lang: init_db(main_db_path(settings.year, settings.month, language=lang))

            }

        # self.post_collection = AnnotPostCollection(settings.languages,
        #                                       settings.year,
        #                                       settings.month,
        #                                       settings.annotation_extra)

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
        lang_or_none = self._methods["filter"].current_result
        if lang_or_none:

            entry = self._create_index_entry(post_data, location_index)
            lang = entry.language
            self.index_entries[lang].append(entry)

            if len(self.index_entries[lang])  > self.DUMP_THRESH:
                with self._language_sessionmakers[lang]() as session:
                    session.add_all(self.index_entries[lang])
                    session.commit()
                    self.index_entries[lang].clear()

    # self.post_collection.add_post(post_data, location_index)

    def finalize(self):
        for lang, sessionmaker in self._language_sessionmakers:
            with self._language_sessionmakers[lang]() as session:
                session.add_all(self.index_entries[lang])
                session.commit()
                self.index_entries[lang].clear()
        # self.post_collection.validate()
        # self.post_collection.finalize_dbs()



class AnnotationDBMethod(IterationMethod):

    @property
    def name(self) -> str:
        return "annotation"

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        pass

    def process_data(self, post_data: dict, location_index: locationindex_type):
        pass

    def finalize(self):
        pass




# self.post_collection.validate()
# self.post_collection.finalize_dbs()


# def main(year: int, month: int, languages: set[str], annotation_extra: str):
#     post_collection = AnnotPostCollection(languages, year, month, annotation_extra)
#
#     # status: CollectionStatus = process_dump(dump_path, session)
#
#     def insert_post(post_data: dict, location_index: tuple[str, str, str, int]):
#         if not check_original_tweet(post_data):
#             return
#         # post = create_annot1(data)
#         if post_data["lang"] not in languages:
#             return
#         post_collection.add_post(post_data, location_index)
#
#     main_generic_all_data(insert_post)


def main(year: int, month: int, languages: set[str], annotation_extra: str):
    settings = IterationSettings(year, month, languages, annotation_extra)
    complex_main_generic_all_data(settings, [PostFilterMethod, StatsCollectionMethod, IndexEntriesDB])


if __name__ == "__main__":
    main(2022, 1, CONFIG.LANGUAGES, ANNOT_EXTRA_TEST_ROUND)
