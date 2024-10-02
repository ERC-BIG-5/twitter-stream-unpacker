from sqlalchemy.orm import sessionmaker

from src.consts import METHOD_INDEX_DB, locationindex_type
from src.db.db import init_db, main_db_path
from src.db.models import DBPostIndexPost
from src.models import IterationSettings
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import post_date, post_url


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

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        status.index_db_available = True
