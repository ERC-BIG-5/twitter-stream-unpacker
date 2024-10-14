from datetime import datetime

from sqlalchemy import create_engine, Integer, String
from sqlalchemy.orm import DeclarativeMeta, declarative_base, sessionmaker, Mapped, mapped_column
from sqlalchemy_utils import create_database
from tqdm import tqdm

from src.consts import BASE_REPACK_PATH, BASE_STAT_PATH
from src.util import read_gzip_file_and_count_lines

Base: DeclarativeMeta = declarative_base()


class RepackStats(Base):
    __tablename__ = 'repack_status'
    id: Mapped[int] = mapped_column(primary_key=True)
    year: Mapped[int] = mapped_column(Integer)
    month: Mapped[int] = mapped_column(Integer)
    day: Mapped[int] = mapped_column(Integer)
    hour: Mapped[int] = mapped_column(Integer)
    minute: Mapped[int] = mapped_column(Integer)
    path: Mapped[str] = mapped_column(String)
    language: Mapped[str] = mapped_column(String)
    count: Mapped[int] = mapped_column(Integer)


def setup_db() -> sessionmaker:
    db_path = BASE_STAT_PATH / "repack_stats.db"
    db_path.unlink(missing_ok=True)
    engine = create_engine(f'sqlite:///{db_path}')
    create_database(engine.url)
    Base.metadata.create_all(engine)
    return sessionmaker(engine)


def repack_stats_main():
    gz_files = list(sorted(BASE_REPACK_PATH.glob("**/*.jsonl.gz")))
    batch_size = 1000
    session_maker = setup_db()
    with session_maker() as session:
        for gz_file in tqdm(gz_files):
            rel_path = gz_file.relative_to(BASE_REPACK_PATH)
            count = read_gzip_file_and_count_lines(gz_file)
            if count == 0:
                continue
            dt = datetime.strptime(gz_file.name.split(".")[0], "%Y%m%d%H%M")
            session.add(RepackStats(
                year=dt.year,
                month=dt.month,
                day=dt.day,
                hour=dt.hour,
                minute=dt.minute,
                path=rel_path.as_posix(),
                language=rel_path.parent.name,
                count=count,
            ))
            if len(session.new) == batch_size:
                session.commit()

        session.commit()


if __name__ == '__main__':
    repack_stats_main()
