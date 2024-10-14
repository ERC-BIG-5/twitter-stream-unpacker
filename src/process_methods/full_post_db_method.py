from typing import Any

from src.consts import locationindex_type
from src.db.models import DBPost
from src.models import IterationSettings
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import post_date, post_url, get_post_text


class FullPostDBMethod(IterationMethod):
    """
    TODO implement
    """

    def __init__(self, settings: IterationSettings, config: dict):
        super().__init__(settings, config)

    @staticmethod
    def name() -> str:
        return "full-posts"

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        pass

    def finalize(self):
        pass

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        pass


def create_main_db_entry(post_data: dict, location_index: list[str]) -> DBPost:
    # previous one, that we need differently later
    # from TimeRangeEvalEntry
    post_dt = post_date(post_data["timestamp"])
    post = DBPost(
        platform="twitter",
        post_url_computed=post_url(post_data),
        date_created=post_dt,
        content=post_data,
        text=get_post_text(post_data),
        language=post_data["lang"],
        location_index=location_index,
    )
    post.set_date_columns()
    return post
