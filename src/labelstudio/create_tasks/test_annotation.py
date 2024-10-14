import json
import shutil
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.consts import logger, BASE_LABELSTUDIO_DATA_PATH
from src.db.db import init_db, main_db_path2
from src.db.models import DBAnnot1Post
from src.models import SingleLanguageSettings


@dataclass
class LabelstudioTask:
    post_text: str
    post_url: Optional[str]
    has_media: bool = field(default=False)
    image: Optional[str] = None


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
        with open(path, "w", encoding="utf-8") as fout:
            json.dump([asdict(task) for task in ls_tasks], fout)
    else:
        path.mkdir(parents=True, exist_ok=True)
        for idx, task in enumerate(ls_tasks):
            (path / f"{str(idx)}.json").write_text(json.dumps(asdict(task), ensure_ascii=False), encoding="utf-8")


def create_annotation_label_ds(settings: SingleLanguageSettings,
                               task_path: Path,
                               single_file: bool = False) -> None:
    session: Session = init_db(main_db_path2(settings))()
    posts = session.execute(select(DBAnnot1Post).order_by(DBAnnot1Post.date_created)).scalars().all()
    label_entries = [
        LabelstudioTask(p.text, p.post_url, p.contains_media or False) for p in posts
    ]
    dump_labelstudio_tasks(label_entries, task_path, single_file)
