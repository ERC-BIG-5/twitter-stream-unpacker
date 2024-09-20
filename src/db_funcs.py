from datetime import datetime

from consts import CONFIG
from db import DBPost


def create_main_db_entry(data: dict, location_index: list[str], time_range_index: int) -> DBPost:
    # previous one, that we need differently later
    # from TimeRangeEvalEntry
    post_dt = datetime.fromtimestamp(int(data["timestamp_ms"] / 1000))
    post = DBPost(
        platform="twitter",
        post_url_computed=f"https://x.com/x/status/{data['id']}",
        date_created=post_dt,
        content=data if CONFIG.STORE_COMPLETE_CONTENT else None,
        text=data["text"],
        language=data["lang"],
        location_index=location_index,
    )
    post.set_date_columns()
    return post