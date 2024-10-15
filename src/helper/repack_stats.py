from itertools import groupby

from sqlalchemy import create_engine, Integer, String, select, DateTime
from sqlalchemy.orm import DeclarativeMeta, declarative_base, sessionmaker, Mapped, mapped_column
from sqlalchemy_utils import create_database
from tqdm import tqdm

from src.consts import BASE_REPACK_PATH, BASE_STAT_PATH
from src.util import read_gzip_file_and_count_lines

Base: DeclarativeMeta = declarative_base()

from datetime import datetime
from typing import List, Tuple


def calculate_group_indices(data: List[Tuple[datetime, int]]) -> List[
    Tuple[int, int, int, int]]:
    # Sort data by datetime and language
    sorted_data = sorted(data, key=lambda x: x[0])

    result = []
    year_counts = {}
    month_counts = {}
    day_counts = {}
    hour_counts = {}

    for dt, count in sorted_data:
        year = dt.year
        month = (dt.year, dt.month)
        day = (dt.year, dt.month, dt.day)
        hour = (dt.year, dt.month, dt.day, dt.hour)

        # Calculate group indices
        year_index = year_counts.get(year, 0)
        month_index = month_counts.get(month, 0)
        day_index = day_counts.get(day, 0)
        hour_index = hour_counts.get(hour, 0)

        # Update counts
        year_counts[year] = year_index + count
        month_counts[month] = month_index + count
        day_counts[day] = day_index + count
        hour_counts[hour] = hour_index + count

        result.append((year_index, month_index, day_index, hour_index))

    return result


class RepackStats(Base):
    __tablename__ = 'repack_status'
    id: Mapped[int] = mapped_column(primary_key=True)
    dt: Mapped[datetime] = mapped_column(DateTime)
    year: Mapped[int] = mapped_column(Integer)
    month: Mapped[int] = mapped_column(Integer)
    day: Mapped[int] = mapped_column(Integer)
    hour: Mapped[int] = mapped_column(Integer)
    minute: Mapped[int] = mapped_column(Integer)
    path: Mapped[str] = mapped_column(String)
    language: Mapped[str] = mapped_column(String)
    count: Mapped[int] = mapped_column(Integer)
    year_group_index: Mapped[int] = mapped_column(Integer, nullable=True)
    month_group_index: Mapped[int] = mapped_column(Integer, nullable=True)
    day_group_index: Mapped[int] = mapped_column(Integer, nullable=True)
    hour_group_index: Mapped[int] = mapped_column(Integer, nullable=True)


def setup_db() -> sessionmaker:
    db_path = BASE_STAT_PATH / "repack_stats.db"
    db_path.unlink(missing_ok=True)
    engine = create_engine(f'sqlite:///{db_path}')
    create_database(engine.url)
    Base.metadata.create_all(engine)
    return sessionmaker(engine)


def repack_stats_main():
    gz_files = list(sorted(BASE_REPACK_PATH.glob("**/*.jsonl.gz")))
    batch_size = 50
    session_maker = setup_db()

    with session_maker() as session:
        for gz_file in tqdm(gz_files):
            rel_path = gz_file.relative_to(BASE_REPACK_PATH)
            count = read_gzip_file_and_count_lines(gz_file)
            if count == 0:
                continue
            dt = datetime.strptime(gz_file.name.split(".")[0], "%Y%m%d%H%M")
            language = rel_path.parent.name
            # g = Group(year=dt.year,month=dt.month,day=dt.day,)
            session.add(RepackStats(
                year=dt.year,
                month=dt.month,
                day=dt.day,
                hour=dt.hour,
                minute=dt.minute,
                path=rel_path.as_posix(),
                language=language,
                count=count,
                dt=dt,
                year_group_index=0
            ))
            if len(session.new) == batch_size:
                session.commit()

        session.commit()

        all_entries = session.execute(
            select(RepackStats).order_by(RepackStats.language, RepackStats.dt)).scalars().all()

        grouped_entries = {
            lang: list(group) for lang, group in groupby(all_entries, key=lambda x: x.language)
        }

        for entries in grouped_entries.values():
            res = calculate_group_indices([(e.dt, e.count) for e in entries])
            for e, count_update in zip(entries, res):
                e.year_group_index, e.month_group_index, e.day_group_index, e.hour_group_index = count_update
                session.add(e)
            session.commit()


if __name__ == '__main__':
    repack_stats_main()
