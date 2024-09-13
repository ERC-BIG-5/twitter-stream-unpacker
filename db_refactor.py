import calendar
from datetime import datetime, timedelta
from typing import Sequence

from sqlalchemy import select, extract, and_, Row
from tqdm import tqdm

from consts import logger
from db_config import Session, create_sqlite_db
from db_models import DBPost


def get_earliest_tweets_by_hour(year, month, day, for_languages: set[str]):
    # Create the date object for the start of the day
    start_date = datetime(year, month, day)

    earliest_tweets = []

    main_session = Session()
    min_session = create_sqlite_db(min=True)()
    # Iterate through each hour of the day
    for hour in tqdm(range(24)):
        for lang in for_languages:
            # Calculate start and end of the hour
            hour_start = start_date + timedelta(hours=hour)
            hour_end = hour_start + timedelta(hours=1)

            # print(list(main_session.execute(select(DBPost).limit(10))))
            # Query for the earliest tweet in this hour
            query = (
                select(DBPost)
                .where(DBPost.date_created >= hour_start)
                .where(DBPost.date_created < hour_end)
                .where(DBPost.language == lang)
                .order_by(DBPost.date_created)
                .limit(1)
            )

            # Execute the query

            result = main_session.execute(query).scalar_one_or_none()
            new_post = DBPost()
            for col in ["platform", "post_url", "post_url_computed", "date_created", "content",
                        "text", "date_collected", "language", "year_created", "month_created", "day_created"]:
                setattr(new_post, col, getattr(result, col))
            min_session.add(new_post)
            # If a tweet was found, add it to our list
            if result:
                earliest_tweets.append(result)
    main_session.close()
    min_session.commit()
    min_session.close()

    return earliest_tweets


def copy_posts_to_new_db(posts: Sequence[Row[tuple[DBPost]]]):
    # Prepare data for bulk insert
    new_posts = []
    for row in posts:
        post = row[0]
        new_post = DBPost()
        for col in ["platform", "post_url", "post_url_computed", "date_created", "content",
                    "text", "date_collected", "language", "year_created", "month_created", "day_created"]:
            setattr(new_post, col, getattr(post, col))
        new_posts.append(new_post)

    min_session = create_sqlite_db(min=True)()
    # Bulk insert into min_session
    min_session.bulk_save_objects(new_posts)
    min_session.commit()

    min_session.close()


def get_earliest_tweets_by_hour_claude(year, month, day, for_languages: set[str]):
    logger.info(f"get earliest tweet: {day}")
    start_date = datetime(year, month, day)
    end_date = start_date + timedelta(days=1)

    main_session = Session()
    min_session = create_sqlite_db(min=True)()

    # Subquery to select tweets for the specific day and languages
    subquery = (
        select(DBPost)
        .where(
            and_(
                DBPost.date_created >= start_date,
                DBPost.date_created < end_date,
                DBPost.language.in_(for_languages)
            )
        )
        .subquery()
    )

    earliest_tweets = []

    # Iterate through each hour of the day
    for hour in tqdm(range(24), desc="Processing hours"):
        for lang in for_languages:
            # Query for the earliest tweet in this hour and language
            query = (
                select(subquery)
                .where(and_(
                    extract('hour', subquery.c.date_created) == hour,
                    subquery.c.language == lang
                ))
                .order_by(subquery.c.date_created)
                .limit(1)
            )

            # Execute the query
            result = main_session.execute(query).scalar_one_or_none()

            if result:
                new_post = DBPost()
                for col in ["platform", "post_url", "post_url_computed", "date_created", "content",
                            "text", "date_collected", "language", "year_created", "month_created", "day_created"]:
                    setattr(new_post, col, getattr(result, col))
                min_session.add(new_post)
                earliest_tweets.append(result)


    main_session.close()

    # Filter out None results (hours with no tweets)
    valid_results = [row for row in earliest_tweets if row[0] is not None]

    copy_posts_to_new_db(valid_results)

    return valid_results

year, month = 2022, 1
max_days = calendar.monthrange(year, month)[1] + 1

for i in range(1,max_days):
    earliest_tweets = get_earliest_tweets_by_hour_claude(year, month, i, {"en", "es", "pt"})
