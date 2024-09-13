from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy_utils import database_exists, create_database

from consts import BASE_DATA_PATH, CONFIG, logger

Base = declarative_base()

def sqlite_db_path(min: bool = False) -> Path:
        subp = CONFIG.SQLITE_MIN_FILE_PATH if min else CONFIG.SQLITE_FILE_PATH
        return (BASE_DATA_PATH / subp).absolute()


def create_sqlite_db(min: bool = False) -> sessionmaker:
    """
    Create the SQLite database and tables based on the defined models.
    """

    engine = create_engine(f'sqlite:///{sqlite_db_path(min).as_posix()}')

    def _fk_pragma_on_connect(dbapi_con, con_record):
        dbapi_con.execute('pragma foreign_keys=ON')

    # Enable foreign key support for SQLite
    event.listen(engine, 'connect', _fk_pragma_on_connect)

    # Create the database if it doesn't exist
    if not database_exists(engine.url):
        logger.debug("create_sqlite_db")
        create_database(engine.url)
        import db_models
        bla = db_models.DBPost
        Base.metadata.create_all(engine)

    return sessionmaker(engine)


def init_db():
    if CONFIG.RESET_DB:
        delete_resp = input(f"Do you want to delete the db? : y/ other key\n")
        if delete_resp == "y":
            logger.info(f"deleting: {sqlite_db_path()}")
            sqlite_db_path().unlink()

    create_sqlite_db()
    create_sqlite_db(True)



Session: sessionmaker = create_sqlite_db()
