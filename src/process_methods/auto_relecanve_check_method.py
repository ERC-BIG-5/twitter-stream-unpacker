from multiprocessing.process import current_process
from typing import Optional, Union, Any

import jsonlines
from pydantic import BaseModel

from src.consts import locationindex_type, METHOD_AUTO_RELEVANCE, AUTO_RELEVANT_COLLECTION, get_logger
from src.models import IterationSettings, SingleLanguageSettings
from src.process_methods.abstract_method import IterationMethod

from src.status import MonthDatasetStatus
from src.util import get_post_text, year_month_lang_str
from word_generator.search import Checker
from word_generator.settings import CheckerConfig

logger = get_logger(__file__)


class AutoRelevanceConfig(BaseModel):
    # we noticed that the first posts for each hour are often automated
    word_list_name: str
    exact_search: Optional[bool] = True
    min_relevant_words: Optional[int] = 2


class AutoRelevanceMethod(IterationMethod):
    """
    only works with repack-iterator cuz of location_index -> language...
    """

    def __init__(self, settings: IterationSettings, config: Union[dict, AutoRelevanceConfig]):
        super().__init__(settings, config)
        if isinstance(config, dict):
            self.config = AutoRelevanceConfig.model_validate(config)
        else:
            self.config: AutoRelevanceConfig = config

        config = CheckerConfig(seed_keyed_vector_file_name=self.config.word_list_name, spacy_model="en_core_web_lg")
        self.relevance_checker = Checker(config)

        self.relevant_sentences: list[tuple[
            locationindex_type, dict, list[str]
        ]] = []
        self.current_lang: Optional[str] = None

    @staticmethod
    def name() -> str:
        return METHOD_AUTO_RELEVANCE

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        relevant_words = self.relevance_checker.exact_search(get_post_text(post_data))
        if relevant_words and len(relevant_words) >= self.config.min_relevant_words:
            if self.current_lang is None:
                self.current_lang = location_index[2]
            if location_index[2] != self.current_lang:
                self.dump()
            self.relevant_sentences.append((location_index, post_data, relevant_words))
            if len(self.relevant_sentences) == 100:
                self.dump()

    def dump(self):
        # first self.relevant_sentences, location_index [2] is language
        logger.info("dumping entries")
        settings = SingleLanguageSettings.from_iter_settings(self.settings, self.relevant_sentences[0][0][2])
        dest = AUTO_RELEVANT_COLLECTION / f"{year_month_lang_str(settings)}.jsonl"
        with dest.open("a", encoding="utf-8") as fout:
            jsonlines.Writer(fout).write_all(self.relevant_sentences)
        self.relevant_sentences.clear()

    def finalize(self):
        self.dump()

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        pass
