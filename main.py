from src.consts import CONFIG, MAIN_STATUS_FILE_PATH, BASE_DBS_PATH, BASE_STAT_PATH
from src.iter_funcs.merge_main_funcs import iter_dumps_main
from src.mutli_func_iter import IterationSettings
from src.status import MainStatus, Main_Status
from src.util import year_month_str


def reset() -> None:
    delete_resp = input(f"Are you sure, you want to reset all data?"
                        f"y/ other key\n")
    if not delete_resp == "y":
        return
    MAIN_STATUS_FILE_PATH.unlink()
    for db in BASE_DBS_PATH.glob("*"):
        db.unlink()
    for stats_file in BASE_STAT_PATH.glob("*"):
        stats_file.unlink()


def main() -> None:
    if CONFIG.RESET_DATA:
        reset()
    main_status = MainStatus.load_status()
    main_status.sync_months()

    ym_s = year_month_str(CONFIG.YEAR, CONFIG.MONTH)
    if ym_s not in main_status.year_months:
        print(f"year month: {ym_s} not included")
        return
    settings = IterationSettings(CONFIG.YEAR, CONFIG.MONTH, CONFIG.LANGUAGES)
    iter_dumps_main(settings, main_status.year_months[ym_s])
    main_status.print_database_statuses()
    if main_status:
        main_status.store_status()


if __name__ == '__main__':
    main()
