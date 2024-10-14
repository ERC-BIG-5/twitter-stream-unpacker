from typing import Optional

from src.consts import CONFIG, logger


def is_original_tweet(post_data: dict) -> bool:
    is_orig = (post_data.get("referenced_tweets") is None and
               post_data.get("in_reply_to_status_id") is None and
               post_data.get("quoted_status_id") is None and
               post_data.get("retweeted_status") is None)

    return is_orig


def check_contains_media(post: dict) -> Optional[bool]:
    for entities_dict_name in ["entities", "extended_entities"]:
        ent_dict = post.get(entities_dict_name, {})
        if "media" in ent_dict:
            return True
    return None


def get_media(post: dict) -> list[str]:
    # TODO analyse how and why we have those 2 keys. how to get the complete content
    for entities_dict_name in ["extended_entities", "entities"]:
        ent_dict = post.get(entities_dict_name, {})
        if "media" in ent_dict:
            media_urls: list[str] = []
            for item in ent_dict["media"]:
                media_urls.append(item['media_url_https'])
            return media_urls
    return []
