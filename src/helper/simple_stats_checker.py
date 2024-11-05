import json
import re
from pathlib import Path

from src.consts import BASE_STAT_PATH



def month_analysis(reg_file: Path, pre_file: Path):
    reg_data = json.load(reg_file.open(encoding="utf-8"))
    pre_data = json.load(pre_file.open(encoding="utf-8"))

    for data in [pre_data, reg_data]:
        total = data["total_posts"]
        sub_tot = 0
        for c in data["accepted_posts"].values():
            sub_tot += c

        print(total, sub_tot )


def check_stats():
    all_json_files = list(BASE_STAT_PATH.glob("*.json"))
    all_pre = list(filter(lambda x: x.name.startswith("pre-"), all_json_files))
    all_regular = filter(lambda x: re.match("^\d{4}-(?:0[1-9]|1[0-2])$", x.stem), all_json_files)

    for reg in all_regular:
        res = list(filter(lambda f: f.stem.lstrip("pre-"), all_pre))
        if res:
            pre_file = res[0]
            month_analysis(reg, pre_file)
        else:
            pre_file = None
        print(reg, pre_file, "-")


if __name__ == "__main__":
    check_stats()
