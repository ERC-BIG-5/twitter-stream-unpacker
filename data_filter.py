

def filter_by_lang(jsonl_data: dict, languages: set[str], orig_tweet: bool = True):
    data = jsonl_data["data"]
    if (data.get("referenced_tweets") is None or not orig_tweet) and data.get("lang") in languages:
        yield data
