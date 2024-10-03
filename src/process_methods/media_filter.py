from typing import Any

from src.consts import METHOD_FILTER, locationindex_type, CONFIG, METHOD_MEDIA_FILTER
from src.models import ProcessCancel
from src.post_filter import is_original_tweet, check_contains_media
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus


class MediaFilterMethod(IterationMethod):
    """
    Filters posts that are in the selected languages and are original
    """

    @property
    def name(self) -> str:
        return METHOD_MEDIA_FILTER

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        if not check_contains_media(post_data):
            return ProcessCancel("no media")
        pass

    def finalize(self):
        pass

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        pass
