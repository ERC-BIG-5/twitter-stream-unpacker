import json
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.consts import LABELSTUDIO_TASK_PATH
from src.db import annotation_db_path, init_db, DBAnnot1Post


@dataclass
class LabelstudioTask:
    post_text: str
    post_url: Optional[str]
    has_media: bool = field(default=False)


def dump_labelstudio_tasks(ls_tasks: list[LabelstudioTask], path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(task) for task in ls_tasks], f)


def get_labelstudio_task_file(year: int, month: int, language: str, annotation_extra: str = "") -> Path:
    f_stem = annotation_db_path(year, month, language, annotation_extra=annotation_extra).stem
    return LABELSTUDIO_TASK_PATH / f"{f_stem}.json"


def create_annotation_label_ds(year: int, month: int, language: str, annotation_extra):
    session: Session = init_db(annotation_db_path(year, month, language, annotation_extra=annotation_extra))()
    posts = session.execute(select(DBAnnot1Post).order_by(DBAnnot1Post.date_created)).scalars().all()
    label_entries = [
        LabelstudioTask(p.text, p.post_url, p.contains_media or False) for p in posts
    ]
    fp = get_labelstudio_task_file(year, month, language, annotation_extra=annotation_extra)
    dump_labelstudio_tasks(label_entries, fp)


if __name__ == '__main__':
    create_annotation_label_ds(2022, 1, "en", "1")
