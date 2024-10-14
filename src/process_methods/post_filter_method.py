from typing import Any, Union

from pydantic import BaseModel

from src.consts import METHOD_FILTER, locationindex_type, CONFIG
from src.models import ProcessCancel, IterationSettings
from src.post_filter import is_original_tweet, check_contains_media
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus


class PostFilterConfig(BaseModel):
    filter_sensitive: bool = False
    filter_no_location: bool = False


class PostFilterMethod(IterationMethod):
    """
    Filters posts that are in the selected languages and are original
    """

    def __init__(self, settings: IterationSettings, config: Union[dict, BaseModel]) -> None:
        super().__init__(settings, config)
        self.config = PostFilterConfig.model_validate(config)

    @staticmethod
    def name() -> str:
        return METHOD_FILTER

    def has_media_filter(self, post_data: dict) -> bool:
        return check_contains_media(post_data)

    def has_location(self, post_data: dict) -> bool:
        return post_data["geo"] is not None or post_data["coordinates"] is not None or post_data["place"] is not None

    def is_truncated(self, post_data: dict) -> bool:
        return post_data["truncated"]

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        # validate that text is present
        # if not post_data.get("extended_tweet") and self.is_truncated(post_data):
        #     print("has NO ExtendedTweet but is truncated")

        if self.config.filter_sensitive and post_data["possibly_sensitive"]:
            return ProcessCancel("filter out: sensitive")
        if self.config.filter_no_location and not self.has_location(post_data):
            return ProcessCancel("filter out: location")
        if post_data.get("lang") in CONFIG.LANGUAGES and is_original_tweet(post_data):
            # print(get_post_text(post_data))
            return post_data.get("lang")
        return ProcessCancel("filtered out")

    def finalize(self):
        pass

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        pass
