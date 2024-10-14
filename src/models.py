from dataclasses import dataclass
from enum import Enum
from typing import Optional, Type, TYPE_CHECKING, Union

from pydantic import BaseModel

if TYPE_CHECKING:
    from src.process_methods.abstract_method import IterationMethod


@dataclass
class IterationSettings:
    year: int
    month: int
    languages: set[str]
    annotation_extra: str = ""


@dataclass
class SingleLanguageSettings:
    year: int
    month: int
    language: str
    annotation_extra: str = ""

    @staticmethod
    def from_iter_settings(iteration_settings: IterationSettings, language: str):
        assert language in iteration_settings.languages
        return SingleLanguageSettings(iteration_settings.year,
                                      iteration_settings.month,
                                      language,
                                      iteration_settings.annotation_extra)


@dataclass
class MethodDefinition:
    method_name: str
    method_type: Optional[Type["IterationMethod"]] = None
    config: Optional[Union[dict, BaseModel]] = None


class ProcessCancel:

    def __init__(self, reason: Optional[str] = ""):
        self.reason = reason


# JSON_FILE: a minute
# TAR_FILE: a day
ProcessSkipType = Enum('ProcessSkipType', ['JSON_FILE', 'TAR_FILE', 'DUMP_FOLDER'])
