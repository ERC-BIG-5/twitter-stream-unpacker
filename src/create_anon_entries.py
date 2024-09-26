import calendar
import json
from datetime import datetime
from typing import Optional

from sqlalchemy import select, func
from sqlalchemy.orm import Session
from tqdm import tqdm

from src.collect_from_time_range_table import get_first_tweets_by_hour
from src.consts import BASE_DATA_PATH, logger
from src.db import DBAnnot1Post, DBPost, init_db, main_db_path, annotation_db_path
from src.util import post_url, post_date


def check_contains_media(post: dict) -> Optional[bool]:
    for entities_dict_name in ["entities", "exextended_entities"]:
        ent_dict = post.get(entities_dict_name, {})
        if "media" in ent_dict:
            return True
        return None


def create_annot1(post: dict) -> DBAnnot1Post:
    db_post = DBAnnot1Post(
        post_url=post_url(post),
        location_index=[],  # todo, pass with the rest
        platform_id=post['id_str'],
        date_created=post_date(post['timestamp_ms']),
        language=post['lang'],
        text=post['text'],
        contains_media=check_contains_media(post)
    )
    db_post.set_date_columns()
    return db_post

def create_annot1_from_post(post: DBPost) -> DBAnnot1Post:
    db_post = create_annot1(post.content)
    db_post.location_index = post.location_index
    return db_post



def create_many_annot1(posts: list[dict]) -> list[DBAnnot1Post]:
    return [create_annot1(post) for post in posts]


def create_annot1_from_complete(year: int, month: int, languages: set[str]):
    main_session = init_db(main_db_path(year=year, month=month))()
    annot_session: Session = init_db(annotation_db_path(year=year, month=month, annotation_extra="1"))()

    try:
        max_days = calendar.monthrange(year, month)[1] + 1
        for day in range(1, max_days):
            logger.info(f"day {day}")
            for lang in languages:
                start = datetime.now()
                logger.info(f"lang {lang}")
                subquery = select(DBPost,
                                  func.row_number().over(
                                      partition_by=DBPost.hour_created,
                                      order_by=DBPost.date_created
                                  ).label('row_num')
                                  ).where(DBPost.day_created == day,
                                          DBPost.language == lang).subquery()

                query = (
                    select(DBPost)
                    .select_from(subquery)
                    # .join("row_num")
                    .where(subquery.c.row_num == 1)
                    .order_by(subquery.c.date_created)
                )

                hour_post = main_session.execute(query).scalars().all()
                annot_post = create_annot1(hour_post)
                annot_session.add(annot_post)
                print(datetime.now() - start, len(annot_post))
    except Exception as e:
        raise e
    finally:
        annot_session.commit()
        main_session.close()
        annot_session.close()


def create_annot1_test_from_complete(year: int, month: int, languages: set[str]):
    main_session = init_db(main_db_path(year=year, month=month))()
    annot_session: Session = init_db(annotation_db_path(year=year, month=month, annotation_extra="1"))()
    check_existing = True

    try:
        max_days = calendar.monthrange(year, month)[1] + 1
        for day in range(1, max_days):
            logger.info(f"day {day}")

            for lang in languages:
                logger.info(f"lang {lang}")

                for hour in tqdm(range(24)):

                    if check_existing:
                        exists_already = annot_session.execute(select(DBAnnot1Post).where(
                            DBAnnot1Post.day_created == day,
                            DBAnnot1Post.language == lang,
                        DBAnnot1Post.hour_created == hour)).one_or_none()
                        if exists_already:
                            continue
                        check_existing = False

                    logger.info(f"hour {hour}")
                    final_query = select(DBPost).where(
                        DBPost.day_created == day,
                        DBPost.language == lang,
                        DBPost.hour_created == hour).order_by(
                        DBPost.date_created).limit(1)
                    hour_post = main_session.execute(final_query).scalar_one_or_none()
                    if hour_post:
                        annot_post = create_annot1_from_post(hour_post)
                        annot_session.add(annot_post)
                    annot_session.commit()
    except Exception as e:
        raise e
    finally:
        main_session.close()


def main_create_annot1_db():
    posts = get_first_tweets_by_hour(2022, 3, {"en"}, 1)
    posts = json.load((BASE_DATA_PATH / "TEMP_MAR.json").open())
    # create_annot1__from_time_range_posts(posts)
    # json.dump(posts, (BASE_DATA_PATH / "TEMP_MAR.json").open("w", encoding="utf-8"))
    # pass


if __name__ == "__main__":
    # main_create_annot1_db()
    # create_annot1_from_complete(2022, 1, {"en"})
    create_annot1_test_from_complete(2022, 1, {"en"})
