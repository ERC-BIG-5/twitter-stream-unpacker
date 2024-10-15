from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Optional


from src.models import IterationSettings
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus


class BaseIterator(ABC, Iterable):

    def __init__(self, settings: IterationSettings,
                 status: Optional[MonthDatasetStatus],
                 methods: list[IterationMethod]):

        self.settings = settings
        self.status = status
        self.methods = methods

    @abstractmethod
    def __iter__(self):
        pass

    def __next__(self):
        # This method is not strictly necessary if __iter__ returns an iterator
        return next(self.__iter__())

    def __del__(self):
        # Ensure the session is closed when the object is garbage collected
        if self.session:
            self.session.close()