import calendar
from copy import copy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from src.consts import BASE_REPACK_PATH, CONFIG


def d2_rjust(m: int) -> str:
    return str(m).rjust(2, '0')


def get_missing_additional(expected_set: set[str], existing_set: set[str], allow_missing: set[str] = None) -> tuple[
    list[str], list[str], bool]:
    if not allow_missing:
        allow_missing = set()
    missing = expected_set - existing_set
    additional = existing_set - expected_set
    tolerance = existing_set | allow_missing
    all_good_ = expected_set == tolerance and len(expected_set - tolerance) == 0
    return sorted(missing - tolerance), sorted(additional), all_good_


def validate_lang_day_folder(lang_day_folder: Path, time_group_resolution: int = 15):
    # get the files that we expect:
    day_str = lang_day_folder.parent.name

    snapshot_dt =  datetime.strptime(day_str, "%Y%m%d")
    origin_day = copy(snapshot_dt)
    snapshot_interval = timedelta(seconds=time_group_resolution*60)
    expected_ranges:list[str] = []

    while snapshot_dt - origin_day < timedelta(days=1):
        expected_ranges.append(f'{snapshot_dt.strftime("%Y%m%d%H%M")}.jsonl.gz')
        snapshot_dt = snapshot_dt + snapshot_interval
    existing_ranges = set([n.name for n in lang_day_folder.glob("*")])
    missing_ranges, additional_ranges, all_good = get_missing_additional(set(expected_ranges),
                                                                         existing_ranges)
    if not all_good:
        print(day_str, lang_day_folder.name)
        if missing_ranges:
            print("Missing ranges", missing_ranges)
        if additional_ranges:
            print("Additional ranges folders", additional_ranges)


def validate_day_folder(day_folder_path: Path):

    expected_language_folder_names = set(CONFIG.LANGUAGES)
    existing_lang_folders = set([n.name for n in day_folder_path.glob("*")])
    missing_days, additional_days, all_good = get_missing_additional(expected_language_folder_names,
                                                                     existing_lang_folders, {"zxx"})
    # print(day_folder_path.name)
    # print(day_folder_path.name, missing_days, additional_days, all_good)
    if not all_good:
        print(day_folder_path.name, missing_days)

    lang_folders_to_check = sorted(existing_lang_folders & expected_language_folder_names)
    for lang_folder in lang_folders_to_check:
        validate_lang_day_folder(day_folder_path / lang_folder)


def validate_repack(year: int, month: Optional[int] = None):

    if month:
        expected_months_folders = {f"{year}-{d2_rjust(month)}"}
    else:
        expected_months_folders = set([
            f"{year}-{d2_rjust(m)}"
            for m in range(1, 13)
        ])
    existing_months_folders = set([n.name for n in BASE_REPACK_PATH.glob("*")])
    missing_months, additional_months, all_good = get_missing_additional(expected_months_folders,
                                                                         existing_months_folders)
    if not all_good:
        if missing_months:
            print("Missing months folders", missing_months)
        if additional_months:
            print("Additional months folders", additional_months)

    print("-------")
    month_folders_to_check = sorted(existing_months_folders & expected_months_folders)
    for folder_name in month_folders_to_check:
        print(f"*******\n{folder_name}\n*******")
        month_folder: Path = BASE_REPACK_PATH / folder_name
        month = int(folder_name.split("-")[1])
        expected_days = list(range(1, 1 + calendar.monthrange(year, month)[1]))
        # print(expected_days)
        expected_days_folders = set([f"{year}{d2_rjust(month)}{d2_rjust(e)}" for e in expected_days])
        existing_days_folders = set([n.name for n in month_folder.glob("*")])
        missing_days, additional_days, all_good = get_missing_additional(expected_days_folders,
                                                                         existing_days_folders)
        if not all_good:
            if missing_days:
                print("Missing days folders", missing_days)
            if additional_days:
                print("Additional days folders", additional_days)

        print("--------")
        day_folders_to_check = sorted(existing_days_folders & expected_days_folders)
        for day_folder_name in day_folders_to_check:
            validate_day_folder(month_folder / day_folder_name)
            # for day_folder_name in expected_days_folder_names:
    #print(month_folders_to_check)


if __name__ == '__main__':
    validate_repack(2022, 1)
