import sys
from typing import Optional, Union

from pydantic import BaseModel
from sqlalchemy import select, insert
from sqlalchemy.orm import Session

# from sqlalchemy.dialects.postgresql import insert

from src.consts import METHOD_INDEX_DB, locationindex_type, ENV_SETTINGS, INDEX_DB_BASE_PATH
from src.db.db import init_pg_db, init_db
from src.db.models import DBPostIndexPost
from src.models import IterationSettings
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import post_date

class IndexEntries(BaseModel):
    dump_tresh: int = 500

class IndexEntriesDB(IterationMethod):
    """
    Create an index db entry, that allows to look up
    """

    @staticmethod
    def name() -> str:
        return METHOD_INDEX_DB

    def __init__(self, settings: IterationSettings, config: Optional[Union[BaseModel, dict]]):
        super().__init__(settings, config)
        if isinstance(config, dict):
            self.config = IndexEntries.model_validate(config)
        else:
            self.config = config

        if ENV_SETTINGS.INDEX_DB == "postgres":
            self.session = init_pg_db()()
        else:
            p = INDEX_DB_BASE_PATH / f"{settings.year}-{settings.month}.sqlite"
            if ENV_SETTINGS.TEST_MODE:
                if p.exists():
                    p.unlink()
            self.session:Session = init_db(p,new=True)()

        self.stored_entries: list[DBPostIndexPost] = []

    @staticmethod
    def _create_index_entry(post_data: dict, location_index: locationindex_type, info:dict = None) -> DBPostIndexPost:
        post_dt = post_date(post_data['timestamp_ms'])
        post = DBPostIndexPost(
            # platform="twitter",
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
        self.stored_entries.append(entry)
        # self.session.add(entry)


        # stmt = insert(DBPostIndexPost).prefix_with('OR IGNORE')
        # stmt = insert(entry).on_conflict_do_nothing(index_elements=['platform_id'])

        if len(self.stored_entries) >= self.config.dump_tresh:
            all_ids = [e.platform_id for e in self.stored_entries]
            search_stmt = select(DBPostIndexPost.platform_id).where(DBPostIndexPost.platform_id.in_(all_ids))
            all_dupl_ids = self.session.execute(search_stmt).scalars().all()
            filtered_entries = list(filter(lambda e: e.platform_id not in all_dupl_ids, self.stored_entries))
            self.session.add_all(filtered_entries)
            try:
                self.session.commit()
                self.stored_entries.clear()
            except Exception as e:
                pass
        #     self.session.execute(stmt, {
        #         'platform_id': entry.platform_id,
        #     })
        # pass
        #     self.session.commit()
            # CHECK THE DB
            # sys.exit(0)


    def finalize(self):
        pass
        # self.session.commit()
        # self.session.close()

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        status.index_db_available = True

    def print_outputs(self):
        print(f"dumping post indices to {self.session.bind.url}")
