from typing import Any

from src.consts import METHOD_FILTER, locationindex_type, CONFIG
from src.models import ProcessCancel
from src.post_filter import is_original_tweet
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus


class PostFilterMethod(IterationMethod):
    """
    Filters posts that are in the selected languages and are original
    """

    @property
    def name(self) -> str:
        return METHOD_FILTER

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        if post_data.get("lang") in CONFIG.LANGUAGES and is_original_tweet(post_data):
            return post_data.get("lang")
        return ProcessCancel("filtered out")

    def finalize(self):
        pass

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        pass
