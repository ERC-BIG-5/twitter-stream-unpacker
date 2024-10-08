from abc import ABC, abstractmethod
from typing import Optional, Any, Type, Union

from pydantic import BaseModel

from src.consts import locationindex_type, DATA_SOURCE_DUMP, DATA_SOURCE_REPACK, CONFIG
from src.models import IterationSettings, ProcessCancel, MethodDefinition
from src.status import MonthDatasetStatus, MainStatus


class IterationMethod(ABC):

    def __init__(self, settings: IterationSettings, config: Optional[Union[BaseModel, dict]]):
        self.settings = settings
        self.main_status: Optional[MainStatus] = None
        self.config = config
        self._methods: dict[str, "IterationMethod"] = {}
        self.current_result: Optional[Any] = None

    def set_methods(self, methods: dict[str, "IterationMethod"]):
        self._methods = methods

    def process_data(self, post_data: dict, location_index: locationindex_type) -> Optional[ProcessCancel]:
        self.current_result = self._process_data(post_data, location_index)
        if isinstance(self.current_result, ProcessCancel):
            return self.current_result
        return None

    @staticmethod
    def compatible_with_data_sources() -> list[str]:
        return [DATA_SOURCE_DUMP, DATA_SOURCE_REPACK]

    @staticmethod
    @abstractmethod
    def name() -> str:
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


def get_method_type(method_def: MethodDefinition) -> Type[IterationMethod]:
    if method_def.method_type:
        return method_def.method_type
    else:
        raise NotImplemented("getting method type by its name is not implemented")


def create_methods(settings: IterationSettings, methods: list[MethodDefinition]) -> list[IterationMethod]:
    method_types = [get_method_type(m) for m in methods]
    # filter out methods that are not working with this data_source
    compatible_types = list(filter(lambda m: CONFIG.DATA_SOURCE in m.compatible_with_data_sources(),
                                   method_types))
    _methods = [method_type(settings, m_definition.config or {}) for method_type, m_definition in
                list(zip(compatible_types, methods))]
    _method_dict = {method.name: method for method in _methods}
    for method in _methods:
        method.set_methods(_method_dict)
    return _methods
