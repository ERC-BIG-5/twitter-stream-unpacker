from typing import Optional

import requests


def is_original_tweet(post_data: dict) -> bool:
    is_orig = (post_data.get("referenced_tweets") is None and
               post_data.get("in_reply_to_status_id") is None and
               post_data.get("quoted_status_id") is None and
               post_data.get("retweeted_status") is None)

    return is_orig


def check_contains_media(post: dict) -> Optional[bool]:
    for tweet in [post, post.get("extended_tweet", {})]:
        for entities_dict_name in ["entities", "extended_entities"]:
            ent_dict = tweet.get(entities_dict_name, {})
            if "media" in ent_dict:
                return True
    return False


def get_media(post: dict) -> set[str]:
    # TODO analyse how and why we have those 2 keys. how to get the complete content
    urls = set()
    for post_data in [post, post.get("extended_tweet", {})]:
        for entities_dict_name in ["entities", "extended_entities"]:
            ent_dict = post_data.get(entities_dict_name, {})
            if "media" in ent_dict:
                for item in ent_dict["media"]:
                    urls.add(item['media_url_https'])
    return urls


def remove_user(post: dict) -> None:
    del post["user"]
    if "extendet_tweet" in post and "user" in post["extendet_tweet"]:
        del post["extendet_tweet"]["user"]


def download_media(post: dict) -> list[tuple[str, bytes]]:
    urls = get_media(post)
    # print(urls)
    results = []
    for url in urls:
        resp = requests.get(url)
        if resp.status_code == 200:
            results.append((url.split(".")[-1], resp.content))
        else:
            results.append(None)
    return results
