from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.consts import ANNOTATED_BASE_PATH
from src.db import annotation_db_path, init_db, DBAnnot1Post, Annot1Relevant


def get_analysed_files(year: int, month: int, language: str, annotation_extra: str = "") -> list[Path]:
    f_stem = annotation_db_path(year, month, language, annotation_extra=annotation_extra).stem
    return list((ANNOTATED_BASE_PATH / f_stem).glob("*.sqlite"))

def analyse_ds(year: int, month: int, language: str, annotation_extra):
    dbs: list[Path] = get_analysed_files(year, month, language, annotation_extra)
    for db in dbs:
        db_session: Session = init_db(db, read_only=False)()
        entries = db_session.execute(select(DBAnnot1Post).limit(4)).all()
        for e in entries:
            print(e)
        # entries.text_relevant = Annot1Relevant.RELEVANT
        # db_session.add(entries)
        # db_session.commit()
        # db_session.close()
        # for e in entries:
        #     print(e)


if __name__ == "__main__":
    analyse_ds(2022, 1, "en", "1")
