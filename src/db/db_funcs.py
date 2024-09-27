from datetime import datetime
from typing import Type

from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.sql.functions import session_user

from src.consts import CONFIG, logger
from src.db.models import DBPost, DBUser


def create_main_db_entry(data: dict, location_index: list[str], time_range_index: int) -> DBPost:
    # previous one, that we need differently later
    # from TimeRangeEvalEntry
    post_dt = datetime.fromtimestamp(int(data["timestamp_ms"] / 1000))
    post = DBPost(
        platform="twitter",
        post_url_computed=f"https://x.com/x/status/{data['id']}",
        date_created=post_dt,
        content=data if CONFIG.STORE_COMPLETE_CONTENT else None,
        text=data["text"],
        language=data["lang"],
        location_index=location_index,
    )
    post.set_date_columns()
    return post


def consider_drop_table(session: Session, table: Type[DeclarativeBase]):
    delete_resp = input(f"Do you want to drop the table"
                        f"{table.__table__}? : y/ other key\n")
    if delete_resp == "y":
        table.__table__.drop(session.get_bind())

def remove_user(post: DBPost, user_session: Session) -> dict[str,str]:
    content = post.content
    if content:
        id_str = content["user"]["id_str"]
        db_user: DBUser =  user_session.execute(select(DBUser).where(DBUser.id_str == id_str)).scalar().one_or_none()
        if db_user:
            return {
                "id_str": id_str,
                "display_name": content["user"]["display_name"]
            }
        else:
            user_session.add(DBUser(
                id_str=id_str,
                content=content["user"]
            ))
            user_session.commit()
