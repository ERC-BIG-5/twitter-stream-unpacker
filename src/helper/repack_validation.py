import calendar
from pathlib import Path

from src.consts import BASE_REPACK_PATH


def validate_repack(year: int):
    def d2_rjust(m: int) -> str:
        return str(m).rjust(2, '0')

    def get_missing_additional(expected_set: set[str], existing_set: set[str]) -> tuple[list[str], list[str], bool]:
        all_good = expected_set == existing_set and len(expected_set - existing_set) == 0
        return sorted(expected_set - existing_set), sorted(existing_set - expected_set), all_good

    expected_months_folders = set([
        f"{year}-{d2_rjust(m)}"
        for m in range(1, 13)
    ])
    existing_months_folders = set([n.name for n in BASE_REPACK_PATH.glob("*")])
    missing_months, additional_months, all_good = get_missing_additional(expected_months_folders,
                                                                         existing_months_folders)
    if not all_good:
        print("Missing months folders", missing_months)
        print("Additional months folders", additional_months)

    month_folders_to_check = sorted(existing_months_folders & expected_months_folders)
    for folder_name in month_folders_to_check:
        print(f"*******\n{folder_name}\n*******")
        month_folder: Path = BASE_REPACK_PATH / folder_name
        month = int(folder_name.split("-")[1])
        expected_days = list(range(1, 1+ calendar.monthrange(year, month)[1]))
        expected_days_folder_names = set([f"{year}{d2_rjust(month)}{d2_rjust(e)}" for e in expected_days])
        existing_days_folders = set([n.name for n in month_folder.glob("*")])
        missing_days, additional_days, all_good = get_missing_additional(expected_days_folder_names,
                                                                         existing_days_folders)
        if not all_good:
            print("Missing days folders", missing_days)
            print("Additional days folders", additional_days)
        # for day_folder_name in expected_days_folder_names:
        if folder_name == "2022-01":
            break
    print(month_folders_to_check)


if __name__ == '__main__':
    validate_repack(2022)
