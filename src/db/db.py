from datetime import date
from pathlib import Path
from typing import Optional, Type

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database

from src.consts import logger, BASE_DBS_PATH, CONFIG
from src.models import SingleLanguageSettings


def _get_month_short_name(month_number: int) -> str:
    dt = date(year=1, day=1, month=month_number)
    return dt.strftime("%b")


def _db_path(db_type: str, year: int, month: int, language: str = "XXX", platform: str = "twitter") -> str:
    # jan,feb,mar, ...
    # month_short_name = _get_month_short_name(month).lower()
    lang = language.ljust(3, "_")
    return f'{db_type}_{year}_{str(month).rjust(2, "0")}_{lang}_{platform}.sqlite'


def main_db_path(year: int, month: int, language: str = "", annotation_extra: str = "",
                 platform: str = "twitter") -> Path:
    return BASE_DBS_PATH / _db_path(annotation_extra, year, month, language, platform)


def main_db_path2(settings: SingleLanguageSettings,
                  platform: str = "twitter") -> Path:
    return BASE_DBS_PATH / _db_path(settings.annotation_extra, settings.year, settings.month, settings.language,
                                    platform)


def init_db(db_path: Path, read_only: bool = False,
            new: bool = False, tables: Optional[set[Type[DeclarativeBase]]] = None) -> sessionmaker:
    """

    :param db_path:
    :param read_only: DB MUST EXIST
    :return:
    """
    # ask for removal of db file, if config is True
    if new and db_path.exists():
        raise Exception(f"DB already exists: {db_path}")

    db_uri = db_path.as_posix()
    if read_only:
        pass  # todo did not work, but added ?mode=ro to name
        # db_uri += "?mode=ro&uri=true"
        if not Path(db_uri).exists():
            raise FileNotFoundError(f"DB file does not exist: {db_uri}")

    engine = create_engine(f'sqlite:///{db_uri}')
    if not db_path.exists():
        create_database(engine.url)
        logger.info(f"creating db: {db_path.relative_to(BASE_DBS_PATH)}")
        from src.db.models import Base
        if tables:
            Base.metadata.create_all(engine, tables=[cls.__table__ for cls in tables])
        else:
            Base.metadata.create_all(engine)

    return sessionmaker(engine)


def init_pg_db() -> sessionmaker:
    user = CONFIG.PG_USER_NAME
    host = CONFIG.PG_HOSTNAME
    port = CONFIG.PG_PORT
    pwd = CONFIG.PG_PASSWORD.get_secret_value()
    db_name = CONFIG.PG_DB_NAME

    connection_str = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db_name}"
    engine = create_engine(connection_str)

    from src.db.models import Base, DBPostIndexPost, DBPost, DBUser
    Base.metadata.create_all(engine, tables=[cls.__table__ for cls in [DBPostIndexPost, DBPost, DBUser]])

    return sessionmaker(engine)


if __name__ == "__main__":
    pass
