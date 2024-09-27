import json
import shutil
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.consts import LABELSTUDIO_TASK_PATH, logger
from src.db import annotation_db_path, init_db, DBAnnot1Post


@dataclass
class LabelstudioTask:
    post_text: str
    post_url: Optional[str]
    has_media: bool = field(default=False)


def dump_labelstudio_tasks(ls_tasks: list[LabelstudioTask], path: Path,
                           single_file: bool = False,
                           rewrite: bool = True):
    if path.exists():
        if rewrite:
            logger.info(f"Deleting existing labelstudio_tasks file/path: {path}")
            if path.is_dir():
                shutil.rmtree(str(path.absolute()))
            else:
                path.unlink()
        else:
            print(f"labelstudio tasks already exist: {path}, skipping, set rewrite to delete previous data")
            return
    if single_file:
        with open(path, "w", encoding="utf-8") as f:
            json.dump([asdict(task) for task in ls_tasks], f)
    else:
        path.mkdir(parents=True, exist_ok=True)
        for idx, task in enumerate(ls_tasks):
            (path / f"{str(idx)}.json").write_text(json.dumps(asdict(task), ensure_ascii=False), encoding="utf-8")


def get_labelstudio_task_path(year: int, month: int, language: str, annotation_extra: str = "", single_file:bool = False) -> Path:
    f_stem = annotation_db_path(year, month, language, annotation_extra=annotation_extra).stem
    if single_file:
        return LABELSTUDIO_TASK_PATH / f_stem
    else:
        return LABELSTUDIO_TASK_PATH / f"{f_stem}.json"


def create_annotation_label_ds(year: int, month: int, language: str, annotation_extra: str, single_file: bool = False):
    session: Session = init_db(annotation_db_path(year, month, language, annotation_extra=annotation_extra))()
    posts = session.execute(select(DBAnnot1Post).order_by(DBAnnot1Post.date_created)).scalars().all()
    label_entries = [
        LabelstudioTask(p.text, p.post_url, p.contains_media or False) for p in posts
    ]
    fp = get_labelstudio_task_path(year, month, language, annotation_extra=annotation_extra, single_file=single_file)
    dump_labelstudio_tasks(label_entries, fp, single_file)


if __name__ == '__main__':
    create_annotation_label_ds(2022, 1, "en", "1")
