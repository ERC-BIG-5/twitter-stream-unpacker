from datetime import datetime, timedelta

from sqlalchemy import select, func, extract, and_
from sqlalchemy.orm import aliased
from tqdm import tqdm

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

def get_earliest_tweets_by_hour_claude(year, month, day, for_languages: set[str]):
    start_date = datetime(year, month, day)
    end_date = start_date + timedelta(days=1)

    main_session = Session()
    min_session = create_sqlite_db(min=True)()

    # Subquery to rank tweets within each hour and language
    subq = (
        select(
            DBPost,
            func.rank().over(
                partition_by=[extract('hour', DBPost.date_created), DBPost.language],
                order_by=DBPost.date_created
            ).label('rank')
        )
        .where(
            and_(
                DBPost.date_created >= start_date,
                DBPost.date_created < end_date,
                DBPost.language.in_(for_languages)
            )
        )
        .subquery()
    )

    # Alias the subquery
    aliased_subq = aliased(DBPost, subq)

    # Main query to select the earliest tweet for each hour and language
    query = (
        select(aliased_subq)
        .where(subq.c.rank == 1)
        .order_by(extract('hour', aliased_subq.date_created), aliased_subq.language)
    )

    # Execute the query and fetch results
    results = main_session.execute(query).fetchall()

    # Prepare data for bulk insert
    new_posts = []
    for row in results:
        post = row[0]
        new_post = DBPost()
        for col in ["platform", "post_url", "post_url_computed", "date_created", "content",
                    "text", "date_collected", "language", "year_created", "month_created", "day_created"]:
            setattr(new_post, col, getattr(post, col))
        new_posts.append(new_post)

    # Bulk insert into min_session
    min_session.bulk_save_objects(new_posts)
    min_session.commit()

    main_session.close()
    min_session.close()

    return new_posts

year, month, day = 2022, 1, 14  # Example date
for i in range(5,32):
    print(i)
    earliest_tweets = get_earliest_tweets_by_hour(year, month, i, {"en", "es", "pt"})
