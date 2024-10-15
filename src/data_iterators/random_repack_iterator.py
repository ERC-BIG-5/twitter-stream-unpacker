import sys
from typing import Optional

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from src.consts import BASE_STAT_PATH
from src.data_iterators.base_iterator import BaseIterator
from src.helper.repack_stats import RepackStats
from src.models import IterationSettings
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus


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

    def __iter__(self):
        # Create a new session for each iteration
        self.session = self.session_maker()
        stmt = select(RepackStats)
        #stmt.where(RepackStats.language.in_(self.settings.languages))
        self.query = self.session.execute(stmt).scalars().all()
        return self

    def __next__(self):
        if self.query is None:
            raise StopIteration
        try:
            return next(self.query)
        except StopIteration:
            self.session.close()
            self.session = None
            self.query_iterator = None
            raise


