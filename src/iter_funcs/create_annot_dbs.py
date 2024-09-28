import calendar
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from deprecated.create_anon_entries import create_annot1
from src.consts import CONFIG, logger, ANNOT_EXTRA_TEST_ROUND_EXPERIMENT
from src.db.db import annotation_db_path, strict_init_annot_db_get_session
from src.post_filter import check_original_tweet
from src.simple_generic_iter import main_generic_all_data
from src.util import post_date


@dataclass
class AnnotCollectionEntry:
    dt: datetime
    post_data: dict
    location_index: tuple[str, str, str, int]


class AnnotPostCollection:

    def __init__(self, languages: set[str], year: int, month: int, annot_extr: str):
        self._col: dict[str, dict[int, dict[int, Optional[AnnotCollectionEntry]]]] = {}
        self._language_sessions: dict[str, Session] = {}

        for lang in languages:
            self._col[lang] = {}
            num_days = calendar.monthrange(year, month)[1]
            for day in range(1, num_days + 1):
                self._col[lang][day] = {i: None
                                        for i in range(0, 24)
                                        }
            # DBS
            self._language_sessions[lang] = strict_init_annot_db_get_session(
                annotation_db_path(year,
                                   month,
                                   lang,
                                   annot_extr))

    def add_post(self, post_data: dict, location_index: tuple[str, str, str, int]):
        post_date_ = post_date(post_data['timestamp_ms'])
        day = post_date_.day
        hour = post_date_.hour
        post_lang = post_data['lang']
        current = self._col[post_lang][day][hour]
        if not current:
            logger.debug(f"set {day}.{hour}")
            self._col[post_lang][day][hour] = AnnotCollectionEntry(post_date_, post_data, location_index)
        else:
            if post_date_ < current.dt:
                self._col[post_lang][day][hour] = AnnotCollectionEntry(post_date_, post_data, location_index)

    def validate(self):
        for lang, days in self._col.items():
            for day, hours in days.items():
                for hour, col_entry in hours.items():
                    if not col_entry:
                        print(f"Missing post for: {lang}-{day}-{hour}")

    def finalize_dbs(self):
        for lang, days in self._col.items():
            session = self._language_sessions[lang]
            for day, hours in days.items():
                for hour, col_entry in hours.items():
                    if col_entry:
                        session.add(create_annot1(col_entry.post_data, col_entry.location_index))

            session.commit()
            session.close()


def create_anont_dbs(year: int, month: int, languages: set[str], annot_extr: str):
    if CONFIG.TESTMODE:
        logger.info("Running TEST-MODE, aborting after one tar file")
    post_collection = AnnotPostCollection(languages, year, month, annot_extr)

    def insert_post(post_data: dict, location_index: tuple[str, str, str, int]):
        if not check_original_tweet(post_data):
            return
        # post = create_annot1(data)
        if post_data["lang"] not in languages:
            return
        post_collection.add_post(post_data, location_index)

    main_generic_all_data(insert_post)
    post_collection.validate()
    post_collection.finalize_dbs()

    """

    import subprocess
    subprocess.run(["shutdown", "-h", "now"])
    """


if __name__ == "__main__":
    create_anont_dbs(2022, 1, {"en", "es"}, ANNOT_EXTRA_TEST_ROUND_EXPERIMENT)
