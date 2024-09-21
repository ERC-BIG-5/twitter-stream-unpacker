import calendar
from datetime import datetime, timedelta

from sqlalchemy import select
from tqdm import tqdm

from consts import logger, CONFIG
from db import init_db, main_db_path, annotation_db_path, TimeRangeEvalEntry
from pick_data import grab_posts_from_location


# uses TimeRangeEvalEntry to collect tweet data
def get_first_tweets_by_hour(year: int, month: int, for_languages: set[str], pick_k: int = 5) -> list[dict]:
    max_days = calendar.monthrange(year, month)[1] + 1
    main_session = init_db(main_db_path(year, month), read_only=True)()
    min_session = init_db(annotation_db_path(year, month))()

    posts: list[dict] = []

    for day in range(1, max_days):
        logger.info(f"day: {day}")
        # Create the date object for the start of the day
        start_date = datetime(year, month, day)

        posts_to_collect: list[tuple[TimeRangeEvalEntry], int] = []

        # Iterate through each hour of the day
        for hour in tqdm(range(24)):
            for lang in for_languages:
                # Calculate start and end of the hour
                # TODO Use DBPost.hour_created
                hour_start = start_date + timedelta(hours=hour)
                hour_end = hour_start + timedelta(hours=1)

                # print(list(main_session.execute(select(DBPost).limit(10))))
                # Query for the earliest tweet in this hour
                query = (
                    select(TimeRangeEvalEntry)
                    .where(TimeRangeEvalEntry.date_created >= hour_start)
                    .where(TimeRangeEvalEntry.date_created < hour_end)
                    .where(TimeRangeEvalEntry.language == lang)
                    .order_by(TimeRangeEvalEntry.date_created)
                    .limit(pick_k)
                )

                # Execute the query
                hour_posts = main_session.execute(query).scalars().all()
                # each post is part of a tuple
                posts_to_collect.extend(((h, idx) for idx, h in enumerate(hour_posts)))
        # posts_to_collect
        month_str = str(month).rjust(2, "0")
        day_str = str(day).rjust(2, "0")
        dump_path_date_name = f"{year}-{month_str}"
        tar_file_date_name = f"{year}{month_str}{day_str}"
        jsonl_files_and_jsonl_lines = {}
        for post, idx in posts_to_collect:
            # todo could assert location_index 0 and 1
            jsonl_files_and_jsonl_lines.setdefault(post.location_index[2], []).append(post.location_index[3])
        # todo OVERALL the indices need to be included
        posts.extend(grab_posts_from_location((dump_path_date_name, tar_file_date_name, jsonl_files_and_jsonl_lines)))



    main_session.close()
    min_session.close()
    return posts

if __name__ == "__main__":
    get_first_tweets_by_hour(2022, 3, {"en"})#CONFIG.LANGUAGES)

# def copy_posts_to_new_db(post: TimeRangeEvalEntry, session: Session):
#     new_post = DBPost()
#     for col in ["platform", "post_url", "post_url_computed", "date_created", "content",
#                 "text", "date_collected", "language", "year_created", "month_created", "day_created"]:
#         setattr(new_post, col, getattr(post, col))
#
#     session.add(new_post)
#     if len(session.new) > CONFIG.DUMP_THRESH:
#         session.commit()
