from datetime import date
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy_utils import create_database

from consts import BASE_DATA_PATH, CONFIG, logger

Base = declarative_base()


def get_month_short_name(month_number: int) -> str:
    dt = date(year=1, day=1, month=month_number)
    return dt.strftime("%b")


def main_db_path(month_number: int) -> Path:
    # jan,feb,mar, ...
    month_short_name = get_month_short_name(month_number).lower()
    return BASE_DATA_PATH / f'{month_short_name}_twitter.sqlite'


def min_db_path(month_number: int) -> Path:
    month_short_name = get_month_short_name(month_number).lower()
    return BASE_DATA_PATH / f'{month_short_name}_min_twitter.sqlite'



def init_db(db_path: Path, read_only: bool = False):
    """

    :param db_path:
    :param read_only: DB MUST EXIST
    :return:
    """
    # ask for removal of db file, if config is True
    if CONFIG.RESET_DB:
        delete_resp = input(f"Do you want to delete the db? : y/ other key\n")
        if delete_resp == "y":
            logger.info(f"deleting: {db_path}")
            db_path.unlink()

    db_uri = db_path.as_posix()
    if read_only:
        pass # todo did not work, but added ?mode=ro to name
        # db_uri += "?mode=ro&uri=true"
        if not Path(db_uri).exists():
            raise FileNotFoundError(f"DB file does not exist: {db_uri}")

    logger.info(f"init db: {db_uri}")
    engine = create_engine(f'sqlite:///{db_uri}')
    if not db_path.exists():
        create_database(engine.url)
        import db_models

        _ = db_models.DBPost
        Base.metadata.create_all(engine)

    return sessionmaker(engine)
