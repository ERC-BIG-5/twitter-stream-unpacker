from datetime import date
from pathlib import Path
from typing import Optional, Type

from deprecated.classic import deprecated
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database

from src.consts import CONFIG, logger, MAIN_DB, ANNOTATION_DB, BASE_DBS_PATH
from src.db.models import Base, DBAnnot1Post


def _get_month_short_name(month_number: int) -> str:
    dt = date(year=1, day=1, month=month_number)
    return dt.strftime("%b")


def _db_path(db_type: str, year: int, month: int, language: str = "XXX", platform: str = "twitter") -> str:
    # jan,feb,mar, ...
    month_short_name = _get_month_short_name(month).lower()
    lang = language.ljust(3, "_")
    return f'{db_type}_{year}_{month_short_name}_{lang}_{platform}.sqlite'


def main_db_path(year: int, month: int, language: str = "", annotation_extra: str = "",
                 platform: str = "twitter") -> Path:
    return BASE_DBS_PATH / _db_path(annotation_extra, year, month, language, platform)


@deprecated(reason="we should only use one db for each month")
def annotation_db_path(year: int, month: int, language: str = "",
                       annotation_extra: str = "",
                       platform: str = "twitter") -> Path:
    return BASE_DBS_PATH / _db_path(f"{ANNOTATION_DB}_{annotation_extra}", year, month, language, platform)


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
        if tables:
            Base.metadata.create_all(engine, tables=[cls.__table__ for cls in tables])
        else:
            Base.metadata.create_all(engine)

    return sessionmaker(engine)


def strict_init_annot_db_get_session(db_path: Path) -> Session:
    return init_db(db_path, reset=True, tables={DBAnnot1Post})()


def check_annot_db_exists(year: int, month: int, language: str = "",
                          annotation_extra: str = "",
                          platform: str = "twitter") -> bool:
    return annotation_db_path(year, month, language, annotation_extra, platform).exists()


if __name__ == "__main__":
    init_db(annotation_db_path(2022, 1, "en", annotation_extra="1"),
            tables={DBAnnot1Post}
            )
