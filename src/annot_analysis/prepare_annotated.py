from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import sqlalchemy
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.consts import ANNOTATED_BASE_PATH, BASE_DATA_PATH
from src.db.db import annotation_db_path, init_db
from src.db.models import Annot1Relevant, Annot1Corine, DBAnnot1PostFLEX


def get_annotation_folder(year: int, month: int, language: str, annotation_extra: str = "") -> Path:
    f_stem = annotation_db_path(year, month, language, annotation_extra=annotation_extra).stem
    return ANNOTATED_BASE_PATH / f_stem


def get_analysed_files(year: int, month: int, language: str, annotation_extra: str = "") -> list[Path]:
    annotation_folder = get_annotation_folder(year, month, language, annotation_extra=annotation_extra)
    if not annotation_folder.exists():
        print(f"{annotation_folder.relative_to(BASE_DATA_PATH)} does not exist")
        return []
    return list(annotation_folder.glob("*.sqlite"))


@dataclass
class RowResult:
    """
    dict: {class: [name]}
    """
    text_relevant: dict[Annot1Relevant, list[str]] = field(default_factory=dict)
    text_class: dict[Annot1Corine, list[str]] = field(default_factory=dict)
    media_relevant: dict[Annot1Relevant, list[str]] = field(default_factory=dict)
    media_class: dict[Annot1Corine, list[str]] = field(default_factory=dict)

    def dict(self):
        return {col[0]: {
            clz.value: coders for clz, coders in getattr(self, col[0]).items()
        }
            for col in annot_groups
        }


annot_groups = [("text_relevant", Annot1Relevant),
                ("text_class", Annot1Corine),
                ("media_relevant", Annot1Relevant),
                ("media_class", Annot1Corine)]


def fix(col, val) -> str:
    # print(col,ec,val)
    if col == "text_relevant":
        if val in ["y", "R"]:
            return "r"
        elif val == "n":
            return "n"
    return val


def prepare_sqlite_annotations(year: int, month: int, language: str, annotation_extra: Optional[str] = None) -> dict[
    str, RowResult]:
    dbs: list[Path] = get_analysed_files(year, month, language, annotation_extra)
    if not dbs:
        print("no databases")
    # coder, rows
    broken_rows: dict[str, list[int]] = {}
    results: dict[str, RowResult] = {}

    for db in dbs:
        coder = db.stem.split("_")[-1]
        broken: list[int] = []
        broken_rows[coder] = broken
        db_session: Session = init_db(db, read_only=False)()

        # we need to change the table-name to..._flex which corresponds to a table without enums
        engine = db_session.get_bind()
        insp = sqlalchemy.inspect(engine)
        # print(engine.table_names())
        if not insp.has_table("annot1_post_flex"):
            if not insp.has_table("annot1_post"):
                print(f"ERROR: {db} has no 'annot1_post' table")
                continue
            with engine.connect() as con:
                con.execute(sqlalchemy.text("ALTER TABLE annot1_post RENAME TO annot1_post_flex;"))
            # recreate ??
            # Base.metadata.create_all(engine, tables=[cls.__table__ for cls in tables])

        entries = db_session.execute(select(DBAnnot1PostFLEX)).scalars().all()

        for e in entries:
            row_results = results.setdefault(str(e.id), RowResult())
            for col, ec in annot_groups:
                try:
                    val = getattr(e, col)
                    if not val:
                        continue
                    val = fix(col, val)
                    attr = ec(val)
                    getattr(row_results, col).setdefault(attr, []).append(coder)
                    # class only when its marked relevant
                    if col.endswith("_class"):
                        relevant_col = col.split("_")[0] + "_relevant"
                        if fix(relevant_col, getattr(e, relevant_col)) == "n":
                            print(f"{coder}: entry {e.id}: has '{col}' set but not '{relevant_col}'")
                            broken.append(e.id)
                            continue
                except ValueError:
                    print(f"{coder}: entry {e.id}: has wrong '{col}' value: {getattr(e, col)}")
                    broken.append(e.id)

        # print(broken_rows)
    return results
