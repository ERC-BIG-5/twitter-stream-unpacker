from datetime import date
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy_utils import create_database

from consts import BASE_DATA_PATH, CONFIG, logger

Base = declarative_base()

def get_month_short_name(month_number: int)-> str:
    dt = date(year=1, day=1, month=month_number)
    return dt.strftime("%b")


def init_main_db(month_number: int) -> sessionmaker:
    # jan,feb,mar, ...
    month_short_name = get_month_short_name(month_number)
    db_path = BASE_DATA_PATH / f'{month_short_name}_twitter.sqlite'
    return init_db(db_path)

def init_min_db(month_number: int) -> sessionmaker:
    month_short_name = get_month_short_name(month_number)
    db_path = BASE_DATA_PATH / f'{month_short_name}_min_twitter.sqlite'
    return init_db(db_path)

def init_db(db_path: Path):
    # ask for removal of db file, if config is True
    if CONFIG.RESET_DB:
        delete_resp = input(f"Do you want to delete the db? : y/ other key\n")
        if delete_resp == "y":
            logger.info(f"deleting: {db_path}")
            db_path.unlink()

    engine = create_engine(f'sqlite:///{db_path.as_posix()}')
    if not db_path.exists():
        create_database(engine.url)
        import db_models

        _ = db_models.DBPost
        Base.metadata.create_all(engine)

    return sessionmaker(engine)