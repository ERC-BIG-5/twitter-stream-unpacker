from datetime import date
from datetime import datetime
from enum import Enum as PyEnum
from pathlib import Path
from typing import Optional, Type

from sqlalchemy import String, DateTime, JSON, Integer, func, Boolean, SmallInteger, Enum
from sqlalchemy import create_engine
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeMeta, DeclarativeBase
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy_utils import create_database

from src.consts import BASE_DATA_PATH, CONFIG, logger, MAIN_DB, ANNOTATION_DB

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




class TimeRangeEvalEntry(Base):
    __tablename__ = 'time_range_eval_post'
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


def _get_month_short_name(month_number: int) -> str:
    dt = date(year=1, day=1, month=month_number)
    return dt.strftime("%b")


def _db_path(db_type: str, year: int, month: int, language: str = "XXX", platform: str = "twitter") -> str:
    # jan,feb,mar, ...
    month_short_name = _get_month_short_name(month).lower()
    lang = language.ljust(3, "_")
    return f'{db_type}_{year}_{month_short_name}_{lang}_{platform}.sqlite'


def main_db_path(year: int, month: int, language: str = "", platform: str = "twitter") -> Path:
    return BASE_DATA_PATH / _db_path(MAIN_DB, year, month, language, platform)


def annotation_db_path(year: int, month: int, language: str = "",
                       annotation_extra: str = "",
                       platform: str = "twitter") -> Path:
    return BASE_DATA_PATH / _db_path(f"{ANNOTATION_DB}_{annotation_extra}", year, month, language, platform)


def init_db(db_path: Path, reset: bool = False, read_only: bool = False,
            new: bool = False, tables: Optional[list[Type[DeclarativeBase]]] = None) -> sessionmaker:
    """

    :param db_path:
    :param read_only: DB MUST EXIST
    :return:
    """
    # ask for removal of db file, if config is True
    if new and db_path.exists():
        raise Exception(f"DB already exists: {db_path}")
    if (CONFIG.RESET_DB or reset) and db_path.exists():
        delete_resp = input(f"Do you want to delete the db"
                            f"{db_path}? : y/ other key\n")
        if delete_resp == "y":
            logger.info(f"deleting: {db_path}")
            db_path.unlink()

    db_uri = db_path.as_posix()
    if read_only:
        pass  # todo did not work, but added ?mode=ro to name
        # db_uri += "?mode=ro&uri=true"
        if not Path(db_uri).exists():
            raise FileNotFoundError(f"DB file does not exist: {db_uri}")

    logger.info(f"init db: {db_uri}")
    engine = create_engine(f'sqlite:///{db_uri}')
    if not db_path.exists():
        create_database(engine.url)
        if tables:
            Base.metadata.create_all(engine, tables=[cls.__table__ for cls in tables])
        else:
            Base.metadata.create_all(engine)

    return sessionmaker(engine)

class DBUser(Base):
    __tablename__ = 'user'
    id: Mapped[int] = mapped_column(primary_key=True)
    id_str: Mapped[[str]] = mapped_column(String, nullable=False, unique=True)
    content: Mapped[dict] = mapped_column(JSON, nullable=False)



if __name__ == "__main__":
    init_db(annotation_db_path(2022, 1, "en", annotation_extra="1"),
            reset=True,
            tables=[DBAnnot1Post]
            )
