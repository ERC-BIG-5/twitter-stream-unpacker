import sys
from typing import Optional, Union

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.consts import METHOD_INDEX_DB, locationindex_type
from src.db.db import init_pg_db
from src.db.models import DBPostIndexPost
from src.models import IterationSettings
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import post_date


class IndexEntriesDB(IterationMethod):
    """
    Create an index db entry, that allows to look up
    """

    @staticmethod
    def name() -> str:
        return METHOD_INDEX_DB

    def __init__(self, settings: IterationSettings, config: Optional[Union[BaseModel, dict]]):
        super().__init__(settings, config)

        self.DUMP_THRESH = 10000

        self.session = init_pg_db()()

    @staticmethod
    def _create_index_entry(post_data: dict, location_index: locationindex_type, info:dict = None) -> DBPostIndexPost:
        post_dt = post_date(post_data['timestamp_ms'])
        post = DBPostIndexPost(
            platform="twitter",
            platform_id= post_data["id_str"],
            #post_url_computed=post_url(post_data),
            date_created=post_dt,
            language=post_data["lang"],
            location_index=list(location_index),
            info=info or {}
        )
        return post

    def _process_data(self, post_data: dict, location_index: locationindex_type):
        #lang = entry.language
        #self.index_entries[lang].append(entry)

        # .on_conflict_do_nothing(index_elements=['email'])

        # stmt = select(DBPostIndexPost).where(
        #     DBPostIndexPost.platform_id == post_data["id_str"]
        # )
        # exists = self.session.execute(stmt).scalars().all()
        # if not exists:
        entry = self._create_index_entry(post_data, location_index)
        # self.session.add(entry)
        # if len(self.session.new) > self.DUMP_THRESH:
        #     self.session.commit()
        stmt = insert(DBPostIndexPost).on_conflict_do_nothing(index_elements=['platform_id'])
        self.session.execute(stmt, {
            'platform_id': entry.platform_id,
        })
        if len(self.session.new) > self.DUMP_THRESH:
            self.session.commit()
            # CHECK THE DB
            sys.exit(0)


    def finalize(self):
        pass
        # self.session.commit()
        # self.session.close()

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        status.index_db_available = True

    def print_outputs(self):
        print(f"dumping post indices to {self.session.bind.url}")
