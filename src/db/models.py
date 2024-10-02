from datetime import datetime
from enum import Enum as PyEnum

from sqlalchemy import String, DateTime, JSON, SmallInteger, func, Integer, Boolean, Enum
from sqlalchemy.orm import DeclarativeMeta, declarative_base, Mapped, mapped_column

Base: DeclarativeMeta = declarative_base()


class Annot1Relevant(PyEnum):
    RELEVANT = 'r'
    NOT_RELEVANT = 'n'
    UNCERTAIN = 'u'
    NOT_APPLICABLE = 'na'


class Annot1Corine(PyEnum):
    ARTIFICIAL_SURFACES = "as"
    AGRICULTURAL = "ag"
    FOREST_AND_SEMINATURAL_AREAS = "fsn"
    WETLANDS = "wl"
    WATER_BODIES = "wb"
    NOT_IDENTIFIABLE = "ni"
    AMBIGUOUS = "am"


class DBPostIndexPost(Base):
    __tablename__ = 'postindex'
    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    post_url_computed: Mapped[str] = mapped_column(String(60), nullable=False,
                                                   unique=False)  # todo, take proper user as path variable
    date_created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    location_index: Mapped[list] = mapped_column(JSON, nullable=False)
    language: Mapped[str] = mapped_column(String(5), nullable=False)

    year_created: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    month_created: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    day_created: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    hour_created: Mapped[int] = mapped_column(SmallInteger, nullable=False)

    def set_date_columns(self):
        self.year_created = self.date_created.year
        self.month_created = self.date_created.month
        self.day_created = self.date_created.day
        self.hour_created = self.date_created.hour


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

    year_created: Mapped[int] = mapped_column(Integer, nullable=False)
    month_created: Mapped[int] = mapped_column(Integer, nullable=False)
    day_created: Mapped[int] = mapped_column(Integer, nullable=False)
    hour_created: Mapped[int] = mapped_column(Integer, nullable=False)

    classification_relevant: Mapped[bool] = mapped_column(Boolean, nullable=True)
    classification_note: Mapped[str] = mapped_column(String, nullable=True)
    classification_data: Mapped[dict] = mapped_column(JSON, nullable=True)

    # comments: Mapped[list[DBComment]] = relationship(back_populates="post")

    def set_date_columns(self):
        self.year_created = self.date_created.year
        self.month_created = self.date_created.month
        self.day_created = self.date_created.day
        self.hour_created = self.date_created.hour


class DBAnnot1Post(Base):
    __tablename__ = 'annot1_post'
    id: Mapped[int] = mapped_column(primary_key=True)
    location_index: Mapped[list] = mapped_column(JSON, nullable=False)
    platform_id: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    date_created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    language: Mapped[str] = mapped_column(String(5), nullable=False)

    year_created: Mapped[int] = mapped_column(Integer, nullable=False)
    month_created: Mapped[int] = mapped_column(Integer, nullable=False)
    day_created: Mapped[int] = mapped_column(Integer, nullable=False)
    hour_created: Mapped[int] = mapped_column(Integer, nullable=False)

    text: Mapped[str] = mapped_column(String(300))
    contains_media: Mapped[bool] = mapped_column(Boolean, nullable=True)
    post_url: Mapped[str] = mapped_column(String(60), nullable=False)

    text_relevant: Mapped[Annot1Relevant] = mapped_column(Enum(Annot1Relevant), nullable=True)
    text_class: Mapped[Annot1Corine] = mapped_column(Enum(Annot1Corine), nullable=True)
    text_notes: Mapped[str] = mapped_column(String, nullable=True)
    media_relevant: Mapped[Annot1Relevant] = mapped_column(Enum(Annot1Relevant), nullable=True)
    media_class: Mapped[Annot1Corine] = mapped_column(Enum(Annot1Corine), nullable=True)
    media_notes: Mapped[str] = mapped_column(String, nullable=True)

    def set_date_columns(self):
        self.year_created = self.date_created.year
        self.month_created = self.date_created.month
        self.day_created = self.date_created.day
        self.hour_created = self.date_created.hour


class DBAnnot1PostFLEX(Base):
    __tablename__ = 'annot1_post_flex'
    id: Mapped[int] = mapped_column(primary_key=True)
    location_index: Mapped[list] = mapped_column(JSON, nullable=False)
    platform_id: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    date_created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    language: Mapped[str] = mapped_column(String(5), nullable=False)

    year_created: Mapped[int] = mapped_column(Integer, nullable=False)
    month_created: Mapped[int] = mapped_column(Integer, nullable=False)
    day_created: Mapped[int] = mapped_column(Integer, nullable=False)
    hour_created: Mapped[int] = mapped_column(Integer, nullable=False)

    text: Mapped[str] = mapped_column(String(300))
    contains_media: Mapped[bool] = mapped_column(Boolean, nullable=True)
    post_url: Mapped[str] = mapped_column(String(60), nullable=False)

    text_relevant: Mapped[Annot1Relevant] = mapped_column(String(300), nullable=True)
    text_class: Mapped[Annot1Corine] = mapped_column(String(300), nullable=True)
    text_notes: Mapped[str] = mapped_column(String, nullable=True)
    media_relevant: Mapped[Annot1Relevant] = mapped_column(String(300), nullable=True)
    media_class: Mapped[Annot1Corine] = mapped_column(String(300), nullable=True)
    media_notes: Mapped[str] = mapped_column(String, nullable=True)

    def set_date_columns(self):
        self.year_created = self.date_created.year
        self.month_created = self.date_created.month
        self.day_created = self.date_created.day
        self.hour_created = self.date_created.hour


class DBUser(Base):
    __tablename__ = 'user'
    id: Mapped[int] = mapped_column(primary_key=True)
    id_str: Mapped[[str]] = mapped_column(String, nullable=False, unique=True)
    content: Mapped[dict] = mapped_column(JSON, nullable=False)
