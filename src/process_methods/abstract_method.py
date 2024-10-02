from abc import ABC, abstractmethod
from typing import Optional, Any

from src.consts import locationindex_type
from src.models import IterationSettings, ProcessCancel
from src.status import MonthDatasetStatus, MainStatus


class IterationMethod(ABC):

    def __init__(self, settings: IterationSettings):
        self.settings = settings
        self.main_status: Optional[MainStatus] = None
        self._methods = dict[str, "IterationMethod"]
        self.current_result: Optional[Any] = None

    def set_methods(self, methods: dict[str, "IterationMethod"]):
        self._methods = methods

    def process_data(self, post_data: dict, location_index: locationindex_type) -> Optional[ProcessCancel]:
        self.current_result = self._process_data(post_data, location_index)
        if isinstance(self.current_result, ProcessCancel):
            return self.current_result

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        pass

    @abstractmethod
    def finalize(self):
        pass

    @abstractmethod
    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        pass
