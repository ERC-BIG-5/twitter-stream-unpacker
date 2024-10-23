from typing import Optional, Union

from pydantic import BaseModel

from src.consts import METHOD_INDEX_DB, locationindex_type
from src.db.db import init_pg_db
from src.db.models import DBPostIndexPost
from src.models import IterationSettings
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import post_date, post_url


class IndexEntriesDB(IterationMethod):
    """
    Create an index db entry, that allows to look up
    """

    @staticmethod
    def name() -> str:
        return METHOD_INDEX_DB

    def __init__(self, settings: IterationSettings, config: Optional[Union[BaseModel, dict]]):
        super().__init__(settings, config)

        self.DUMP_THRESH = 5000

        self.session = init_pg_db()()

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
        return post

    def _process_data(self, post_data: dict, location_index: locationindex_type):
        entry = self._create_index_entry(post_data, location_index)
        lang = entry.language
        #self.index_entries[lang].append(entry)
        self.session.add(entry)
        if len(self.session.new) > self.DUMP_THRESH:
            self.session.commit()

    def finalize(self):
        self.session.commit()
        self.session.close()

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        status.index_db_available = True

    def print_outputs(self):
        print(f"dumping post indices to {self.session.bind.url}")
