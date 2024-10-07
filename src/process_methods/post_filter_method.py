from typing import Any

from src.consts import METHOD_FILTER, locationindex_type, CONFIG
from src.models import ProcessCancel, IterationSettings
from src.post_filter import is_original_tweet, check_contains_media
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import get_post_text


class PostFilterMethod(IterationMethod):
    """
    Filters posts that are in the selected languages and are original
    """
    def __init__(self, settings: IterationSettings, config: dict) -> None:
        super().__init__(settings, config)

    @property
    def name(self) -> str:
        return METHOD_FILTER

    def has_media_filter(self, post_data: dict) -> bool:
        return check_contains_media(post_data)

    def has_location_filter(self, post_data: dict) -> bool:
        return post_data["geo"] is not None or post_data["coordinates"] is not None or post_data["place"] is not None


    def is_truncated(self, post_data: dict) -> bool:
        return post_data["truncated"]

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        # validate that text is present
        # if not post_data.get("extended_tweet") and self.is_truncated(post_data):
        #     print("has NO ExtendedTweet but is truncated")

        # if self.has_location_filter(post_data):
        #     pass

            #  print(tuple(post_data[k] for k in ["geo", "coordinates", "place"]))
        if post_data.get("lang") in CONFIG.LANGUAGES and is_original_tweet(post_data):
            # print(get_post_text(post_data))
            return post_data.get("lang")
        return ProcessCancel("filtered out")

    def finalize(self):
        pass

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        pass

