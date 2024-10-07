import json
from csv import DictWriter
from dataclasses import asdict
from enum import Enum
from pathlib import Path
from typing import Type

from sqlalchemy import select
from sqlalchemy.orm import Session
from statsmodels.stats.inter_rater import fleiss_kappa

from src.annot_analysis.prepare_annotated import RowResult, prepare_sqlite_annotations, annot_groups, \
    get_annotation_folder, get_analysed_files
from src.db.db import init_db
from src.db.models import DBAnnot1PostFLEX


def restructure_data(results: list[RowResult], field_name: str, enum_class: Type[Enum]) -> list[list[int]]:
    all_coders = set()
    for row in results:
        for coders in getattr(row, field_name).values():
            all_coders.update(coders)

    n_coders = len(all_coders)
    categories = list(enum_class)

    matrix: list[list[int]] = []
    for row in results:
        row_data = [0] * len(categories)
        field_data = getattr(row, field_name)

        for i, category in enumerate(categories):
            if category in field_data:
                row_data[i] = len(field_data[category])

        # Add "not_coded" count
        row_data.append(n_coders - sum(row_data))
        matrix.append(row_data)

    return matrix


def calculate_fleiss_kappa(results: list[RowResult], field_name: str, enum_class: Type[Enum]):
    """
    Fleiss’ and Randolph’s kappa multi-rater agreement measure
    https://www.statsmodels.org/dev/generated/statsmodels.stats.inter_rater.fleiss_kappa.html
    """
    matrix = restructure_data(results, field_name, enum_class)
    # print(matrix)
    kappa = fleiss_kappa(matrix)
    return kappa


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
        agreements[k] = {"value": round(v, 3), "interpretation": interpretation_str(v)}
    return agreements


def select_non_agreements(results: dict[str, RowResult],
                          result_path:Path,
                          entries: list[DBAnnot1PostFLEX]):
    diff_rows:dict[int,list[tuple[str,dict]]] = {}
    all_keys_needed = set()
    for id, row in results.items():
        pass
        for col, data in asdict(row).items():
            if not data:
                continue
            if len(list(data.keys())) > 1:
                transform_data = {k.name: v for k, v in data.items()}
                diff_rows.setdefault(int(id), []).append((col,transform_data))
                for col_val_needed in transform_data.keys():
                    all_keys_needed.add(f"{col}_{col_val_needed}")
    result_file_path = result_path / "results.csv"
    fieldnames = ["id","date_created","text", "post_url"] + list(sorted(all_keys_needed, reverse=True))

    result_rows = []
    for e in entries:
        if e.id in  diff_rows:
            result_row = {
                "id": e.id,
                "date_created": e.date_created,
                "text": e.text,
                "post_url": e.post_url,
            }
            for all_diff_qs in diff_rows[e.id]:
                diff_col, diff = all_diff_qs
                for val, coder in diff.items():
                    result_row[f"{diff_col}_{val}"] = "; ".join(coder)
            result_rows.append(result_row)

    with result_file_path.open("w", encoding="utf-8") as fout:
        writer = DictWriter(fout, fieldnames)
        writer.writeheader()
        for row in result_rows:
            writer.writerow(row)

    return diff_rows


if __name__ == "__main__":
    year, month, lang, extra = 2022, 1, "en", "1"
    results = prepare_sqlite_annotations(year, month, lang, extra)
    # print(results)

    annotation_folder = get_annotation_folder(year, month, lang, extra)
    any_db = get_analysed_files(year, month, lang, extra)[0]
    db_session: Session = init_db(any_db, read_only=False)()
    entries = list(db_session.execute(select(DBAnnot1PostFLEX)).scalars().all())

    select_non_agreements(results, annotation_folder, entries)
    print(json.dumps(calc_agreements(results), indent=2))
