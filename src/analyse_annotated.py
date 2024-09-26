from pathlib import Path

import sqlalchemy
from sqlalchemy import select
from sqlalchemy.orm import Session
import sqlalchemy_utils

from src.consts import ANNOTATED_BASE_PATH
from src.db import annotation_db_path, init_db, DBAnnot1Post, Annot1Relevant, DBAnnot1PostFLEX, Annot1Corine


def get_analysed_files(year: int, month: int, language: str, annotation_extra: str = "") -> list[Path]:
    f_stem = annotation_db_path(year, month, language, annotation_extra=annotation_extra).stem
    return list((ANNOTATED_BASE_PATH / f_stem).glob("*.sqlite"))



def analyse_ds(year: int, month: int, language: str, annotation_extra):
    dbs: list[Path] = get_analysed_files(year, month, language, annotation_extra)
    for db in dbs:
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
            for col, ec in [("text_relevant", Annot1Relevant),
                            ("text_class", Annot1Corine),
                            ("media_relevant", Annot1Relevant),
                            ("media_class", Annot1Corine)]:
                try:
                    val = getattr(e,col)
                    if not val:
                        continue
                    attr = ec(val)
                    # class only when its marked relevant
                    if col.endswith("_class"):
                        relevant_col = col.split("_")[0] + "_relevant"
                        if getattr(e,relevant_col) != "r":
                            print(f"entry {e.id}: has '{col}' set but not '{relevant_col}'")

                except ValueError:
                    print(f"entry {e.id}: has wrong '{col}' value: {getattr(e,col)}")

if __name__ == "__main__":
    analyse_ds(2022, 1, "en", "1")
