import calendar
from datetime import datetime, timedelta

from src.consts import logger, CONFIG
from src.db.db import main_db_path, init_db, annotation_db_path
from src.db.models import DBPost
from sqlalchemy import select, extract, and_
from sqlalchemy.orm import Session
from tqdm import tqdm


def get_earliest_tweets_by_hour(year: int, month: int, for_languages: set[str]):
    max_days = calendar.monthrange(year, month)[1] + 1
    main_session = init_db(main_db_path(month), read_only=True)()
    min_session = init_db(annotation_db_path(month))()
    for day in range(1, max_days):
        logger.info(f"day: {day}")
        # Create the date object for the start of the day
        start_date = datetime(year, month, day)

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
                    select(DBPost)
                    .where(DBPost.date_created >= hour_start)
                    .where(DBPost.date_created < hour_end)
                    .where(DBPost.language == lang)
                    .order_by(DBPost.date_created)
                    .limit(1)
                )

                # Execute the query
                hour_post = main_session.execute(query).scalar_one_or_none()
                if hour_post:
                    copy_posts_to_new_db(hour_post, min_session)
    main_session.close()
    min_session.close()


def copy_posts_to_new_db(post: DBPost, session: Session):
    new_post = DBPost()
    for col in ["platform", "post_url", "post_url_computed", "date_created", "content",
                "text", "date_collected", "language", "year_created", "month_created", "day_created"]:
        setattr(new_post, col, getattr(post, col))

    session.add(new_post)
    if len(session.new) > CONFIG.DUMP_THRESH:
        session.commit()


def get_earliest_tweets_by_hour_claude(year, month, day, for_languages: set[str]):
    logger.info(f"get earliest tweet: {day}")
    start_date = datetime(year, month, day)
    end_date = start_date + timedelta(days=1)

    # TODO...
    # main_session = init_main_db(month)
    # min_session = init_min_db(month)

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
    #         result = main_session.execute(query).scalar_one_or_none()
    #
    #         if result:
    #             new_post = DBPost()
    #             for col in ["platform", "post_url", "post_url_computed", "date_created", "content",
    #                         "text", "date_collected", "language", "year_created", "month_created", "day_created"]:
    #                 setattr(new_post, col, getattr(result, col))
    #             min_session.add(new_post)
    #             earliest_tweets.append(result)
    #
    # main_session.close()

    # Filter out None results (hours with no tweets)
    valid_results = [row for row in earliest_tweets if row[0] is not None]

    copy_posts_to_new_db(valid_results)

    return valid_results


if __name__ == "__main__":
    year = CONFIG.YEAR
    month = CONFIG.MONTH
    get_earliest_tweets_by_hour(year, month, CONFIG.LANGUAGES)
