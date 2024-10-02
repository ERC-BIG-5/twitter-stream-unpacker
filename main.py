from typing import Optional

from src.consts import CONFIG, MAIN_STATUS_FILE_PATH, BASE_DBS_PATH, BASE_STAT_PATH
from src.mutli_func_iter import IterationSettings, complex_main_generic_all_data
from src.process_methods.annotation_db_method import AnnotationDBMethod
from src.process_methods.index_db_method import IndexEntriesDB
from src.process_methods.post_filter_method import PostFilterMethod
from src.process_methods.stats_method import StatsCollectionMethod
from src.status import MainStatus, Main_Status, MonthDatasetStatus
from src.util import year_month_str


def reset() -> None:
    delete_resp = input(f"Are you sure, you want to reset all data?"
                        f"y/ other key\n")
    if not delete_resp == "y":
        return
    if MAIN_STATUS_FILE_PATH.exists():
        MAIN_STATUS_FILE_PATH.unlink()
    for db in BASE_DBS_PATH.glob("*"):
        db.unlink()
    for stats_file in BASE_STAT_PATH.glob("*"):
        stats_file.unlink()


def iter_dumps_main(settings: IterationSettings, month_ds_status: Optional[MonthDatasetStatus]):
    complex_main_generic_all_data(settings, month_ds_status, [PostFilterMethod,
                                                              StatsCollectionMethod,
                                                              IndexEntriesDB,
                                                              AnnotationDBMethod])


def main() -> None:
    if CONFIG.RESET_DATA:
        reset()
    # load status
    main_status = MainStatus.load_status()
    main_status.sync_months()
    # check if selected is available
    ym_s = year_month_str(CONFIG.YEAR, CONFIG.MONTH)
    if ym_s not in main_status.year_months:
        print(f"year month: {ym_s} not included")
        return
    settings = IterationSettings(CONFIG.YEAR, CONFIG.MONTH, CONFIG.LANGUAGES)
    month_status = main_status.year_months[ym_s]
    iter_dumps_main(settings, month_status)
    main_status.print_database_status(month_status)
    if main_status:
        main_status.store_status()


if __name__ == '__main__':
    main()
