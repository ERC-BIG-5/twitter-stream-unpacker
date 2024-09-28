import json
from enum import Enum
from typing import Type

from statsmodels.stats.inter_rater import fleiss_kappa

from src.annot_analysis.prepare_annotated import RowResult, prepare_sqlite_annotations, annot_groups


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
        agreements[k] = {"value": round(v,3), "interpretation": interpretation_str(v)}
    return agreements


if __name__ == "__main__":
    results = prepare_sqlite_annotations(2022, 1, "en", "1")
    # print(results)
    print(json.dumps(calc_agreements(results), indent=2))

    # print(calculate_fleiss_kappa(list(results.values())[:5], "text_relevant", Annot1Relevant))
