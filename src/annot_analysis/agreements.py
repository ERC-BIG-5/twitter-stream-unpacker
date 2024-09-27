from enum import Enum
from typing import Type

from statsmodels.stats.inter_rater import fleiss_kappa

from src.annot_analysis.prepare_annotated import RowResult


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
