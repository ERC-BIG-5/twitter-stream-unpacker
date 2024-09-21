import json
from datetime import datetime

from src.consts import BASE_DATA_PATH
from src.db import DBAnnot1Post

def check_contains_media(post: dict)-> bool:
    pass

def create_annot1__from_time_range_posts(posts: list[dict]) -> list[DBAnnot1Post]:

    for post in posts:
        db_post = DBAnnot1Post(
            post_url=f"x.com/{post['user']['name']}/status/{post['id']}",
            location_index=[], # todo, pass with the rest
            platform_id=post['id_str'],
            date_created=datetime.fromtimestamp(int(post['timestamp_ms'] / 1000)),
            language=post['lang'],
            text=post['text'],
            contain_media=""
        )
        db_post.set_date_columns()
    """
            platform="twitter",
        post_url_computed=f"https://x.com/x/status/{data['id']}",
        date_created=post_dt,
        language=data["lang"],
        location_index=location_index,
    """

def main_create_annot1_db():
    # posts = get_first_tweets_by_hour(2022,3,{"en"},1)
    posts = json.load((BASE_DATA_PATH / "TEMP_MAR.json").open())
    create_annot1__from_time_range_posts(posts)
    #json.dump(posts, (BASE_DATA_PATH / "TEMP_MAR.json").open("w", encoding="utf-8"))
    #pass

if __name__ == "__main__":
    main_create_annot1_db()