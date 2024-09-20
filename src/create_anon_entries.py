from collect_from_time_range_table import get_first_tweets_by_hour
from db import TimeRangeEvalEntry, DBAnnot1Post

def create_annot1__from_time_range_posts(posts: list[TimeRangeEvalEntry]) -> list[DBAnnot1Post]:
    pass

def main_create_annot1_db():
    posts = get_first_tweets_by_hour(2022,1,{"en"},1)
    pass

if __name__ == "__main__":
    main_create_annot1_db()