from typing import Type

from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase, Session

from src.db.models import DBPost, DBUser


def consider_drop_table(session: Session, table: Type[DeclarativeBase]):
    delete_resp = input(f"Do you want to drop the table"
                        f"{table.__table__}? : y/ other key\n")
    if delete_resp == "y":
        table.__table__.drop(session.get_bind())


def remove_user(post: DBPost, user_session: Session) -> dict[str, str]:
    content = post.content
    if content:
        id_str = content["user"]["id_str"]
        db_user: DBUser = user_session.execute(select(DBUser).where(DBUser.id_str == id_str)).scalar().one_or_none()
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
