from datetime import datetime
from enum import Enum as PyEnum

from sqlalchemy import String, DateTime, JSON, SmallInteger, func, Integer, Boolean, Enum
from sqlalchemy.orm import DeclarativeMeta, declarative_base, Mapped, mapped_column

Base: DeclarativeMeta = declarative_base()


class DBPostIndexPost(Base):
    __tablename__ = 'postindex'
    id: Mapped[int] = mapped_column(primary_key=True)
    #platform: Mapped[str] = mapped_column(String(20), nullable=False)
    platform_id: Mapped[str] = mapped_column(String(50), nullable=True, unique=True)
    # post_url_computed: Mapped[str] = mapped_column(String(60), nullable=False,
    #                                                unique=False)  # todo, take proper user as path variable
    date_created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    location_index: Mapped[list] = mapped_column(JSON, nullable=False)
    language: Mapped[str] = mapped_column(String(5), nullable=False)

    info: Mapped[dict] = mapped_column(JSON, nullable=False)

    def __repr__(self) -> str:
        return f"{self.platform_id} / {self.location_index}"

class DBPost(Base):
    __tablename__ = 'post'

    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    platform_id: Mapped[str] = mapped_column(String(50), nullable=True)
    post_url: Mapped[str] = mapped_column(String(60), nullable=True)
    post_url_computed: Mapped[str] = mapped_column(String(60), nullable=False, unique=False)
    date_created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    content: Mapped[dict] = mapped_column(JSON)
    text: Mapped[str] = mapped_column(String(300))
    date_collected: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    language: Mapped[str] = mapped_column(String(5), nullable=False)
    location_index: Mapped[list] = mapped_column(JSON, nullable=False)
    time_range_index: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)

    info: Mapped[dict] = mapped_column(JSON, nullable=False)


class DBUser(Base):
    __tablename__ = 'user'
    id: Mapped[int] = mapped_column(primary_key=True)
    id_str: Mapped[[str]] = mapped_column(String, nullable=False, unique=True)
    content: Mapped[dict] = mapped_column(JSON, nullable=False)
