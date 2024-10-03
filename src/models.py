from dataclasses import dataclass
from typing import Optional


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


class ProcessCancel:

    def __init__(self, reason: Optional[str] = ""):
        self.reason = reason
