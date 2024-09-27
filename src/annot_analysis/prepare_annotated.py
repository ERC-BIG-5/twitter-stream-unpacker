from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import sqlalchemy
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.consts import ANNOTATED_BASE_PATH
from src.db import annotation_db_path, init_db, Annot1Relevant, DBAnnot1PostFLEX, Annot1Corine


def get_annotation_folder(year: int, month: int, language: str, annotation_extra: str = "") -> Path:
    f_stem = annotation_db_path(year, month, language, annotation_extra=annotation_extra).stem
    return ANNOTATED_BASE_PATH / f_stem


def get_analysed_files(year: int, month: int, language: str, annotation_extra: str = "") -> list[Path]:
    return list(get_annotation_folder(year, month, language, annotation_extra).glob("*.sqlite"))


@dataclass
class RowResult:
    """
    dict: {class: [name]}
    """
    text_relevant: dict[Annot1Relevant, list[str]] = field(default_factory=dict)
    text_class: dict[Annot1Corine, list[str]] = field(default_factory=dict)
    media_relevant: dict[Annot1Relevant, list[str]] = field(default_factory=dict)
    media_class: dict[Annot1Corine, list[str]] = field(default_factory=dict)


annot_groups = [("text_relevant", Annot1Relevant),
                ("text_class", Annot1Corine),
                ("media_relevant", Annot1Relevant),
                ("media_class", Annot1Corine)]


def analyse_ds(year: int, month: int, language: str, annotation_extra):
    dbs: list[Path] = get_analysed_files(year, month, language, annotation_extra)

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
                    attr = ec(val)
                    getattr(row_results, col).setdefault(attr, []).append(coder)
                    # class only when its marked relevant
                    if col.endswith("_class"):
                        relevant_col = col.split("_")[0] + "_relevant"
                        if getattr(e, relevant_col) != "r":
                            print(f"entry {e.id}: has '{col}' set but not '{relevant_col}'")
                            broken.append(e.id)
                            continue
                except ValueError:
                    print(f"entry {e.id}: has wrong '{col}' value: {getattr(e, col)}")
                    broken.append(e.id)

        # print(broken_rows)
    return results


def calc_agreements(ana_ds: dict[str, RowResult]):
    from src.annot_analysis.agreements import calculate_fleiss_kappa
    agreements = {}
    ana_ds_list = list(ana_ds.values())
    for col, ec in annot_groups:
        agreements[col] = calculate_fleiss_kappa(ana_ds_list, col, ec)

    def interpretation_str(value: float) -> str:
        if value < 0:
            return "Poor aggreement"
        elif value < 0.2:
            return "Slight agreement"
        elif value < 0.4:
            return "Fair agreement"
        elif value < 0.6:
            return "Moderate agreement"
        elif value < 0.8:
            return "Substantial agreement"
        else:
            return "Almost perfect agreement"

    for k, v in agreements.items():
        agreements[k] = {"value": v, "interpretation": interpretation_str(v)}
    return agreements


if __name__ == "__main__":
    results = analyse_ds(2022, 1, "en", "1")
    # print(results)
    print(calc_agreements(results))

    # print(calculate_fleiss_kappa(list(results.values())[:5], "text_relevant", Annot1Relevant))
