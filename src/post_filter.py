from src.consts import CONFIG, logger
from src.util import post_url


def check_original_tweet(data: dict) -> bool:
    is_orig =(data.get("referenced_tweets") is None and
            data.get("in_reply_to_status_id") is None and
            data.get("quoted_status_id") is None and
            data.get("retweeted_status") is None) or not CONFIG.ONLY_ORIG_TWEETS

    if is_orig and data["text"].startswith("RT"):
#        logger.warning("RT filter broken")
#        logger.warning(post_url(data))
        pass
    return is_orig