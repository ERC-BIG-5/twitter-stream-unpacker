from abc import ABC, abstractmethod
from typing import Optional

from src.models import IterationSettings
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus


class BaseIterator(ABC):

    def __init__(self, settings: IterationSettings,
                 status: Optional[MonthDatasetStatus],
                 methods: list[IterationMethod]):

        self.settings = settings
        self.status = status
        self.methods = methods

    @abstractmethod
    def __iter__(self):
        pass