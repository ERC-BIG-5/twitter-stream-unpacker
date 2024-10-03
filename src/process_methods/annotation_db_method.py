import calendar
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from src.consts import METHOD_ANNOTATION_DB, locationindex_type, METHOD_FILTER, METHOD_MEDIA_FILTER, logger
from src.db.db import init_db, main_db_path
from src.db.models import DBAnnot1Post
from src.models import IterationSettings
from src.post_filter import check_contains_media, get_media
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import post_date, post_url


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
            num_days = calendar.monthrange(year, month)[1] + 1
            for day in range(1, num_days):
                self._col[lang][day] = {i: None
                                        for i in range(0, 24)
                                        }
            # DBS
            self._language_sessions[lang] = init_db(main_db_path(year,
                                                                 month,
                                                                 lang,
                                                                 annot_extr))()

    def add_post(self, post_data: dict, location_index: tuple[str, str, str, int]) -> Optional[AnnotCollectionEntry]:
        """
        put into an hour slot, if would be the newest in there
        return AnnotCollectionEntry if it gets inserted
        """
        post_date_ = post_date(post_data['timestamp_ms'])
        day = post_date_.day
        hour = post_date_.hour
        post_lang = post_data['lang']
        current = self._col[post_lang][day][hour]
        if not current:
            logger.debug(f"set {day}.{hour}")
            ace = AnnotCollectionEntry(post_date_, post_data, location_index)
            self._col[post_lang][day][hour] = ace
            return ace
        else:
            if post_date_ < current.dt:
                ace = AnnotCollectionEntry(post_date_, post_data, location_index)
                self._col[post_lang][day][hour] = ace
                return ace

    def validate(self):
        for lang, days in self._col.items():
            for day, hours in days.items():
                for hour, col_entry in hours.items():
                    if not col_entry:
                        print(f"Missing post for: {lang}-day:{day}-hour:{hour}")

    def create_annot1(self, post: dict,
                      location_index: Optional[tuple[str, str, str, int]] = None) -> DBAnnot1Post:
        db_post = DBAnnot1Post(
            post_url=post_url(post),
            location_index=[],  # todo, pass with the rest
            platform_id=post['id_str'],
            date_created=post_date(post['timestamp_ms']),
            language=post['lang'],
            text=post['text'],
            contains_media=check_contains_media(post),
            extra={"media": get_media(post)}
        )
        db_post.set_date_columns()
        db_post.location_index = location_index
        return db_post

    def finalize_dbs(self):
        for lang, days in self._col.items():
            session = self._language_sessions[lang]
            for day, hours in days.items():
                for hour, col_entry in hours.items():
                    if col_entry:
                        session.add(self.create_annot1(col_entry.post_data, col_entry.location_index))

            session.commit()
            session.close()


class AnnotationDBMethod(IterationMethod):

    def __init__(self, settings: IterationSettings):
        super().__init__(settings)

        self.post_collection = AnnotPostCollection(settings.languages,
                                                   settings.year,
                                                   settings.month,
                                                   settings.annotation_extra)

    @property
    def name(self) -> str:
        return METHOD_ANNOTATION_DB

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        added_post = self.post_collection.add_post(post_data, location_index)

    def finalize(self):
        self.post_collection.validate()
        self.post_collection.finalize_dbs()

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        status.annotated_db_available = True
