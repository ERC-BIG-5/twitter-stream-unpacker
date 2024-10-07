from typing import Optional

from src.consts import CONFIG, MAIN_STATUS_FILE_PATH, BASE_DBS_PATH, BASE_STAT_PATH, logger, DATA_SOURCE_DUMP, \
    DATA_SOURCE_REPACK

from src.data_iterators.base_data_iterator import base_month_data_iterator
from src.data_iterators.repacked_data_iterator import repack_iterator
from src.models import MethodDefinition, IterationSettings
from src.process_methods.abstract_method import IterationMethod, create_methods
from src.process_methods.annotation_db_method import AnnotationDBMethod, AnnotationDBMethodConfig
from src.process_methods.repack_data import PackEntries
from src.process_methods.post_filter_method import PostFilterMethod
from src.process_methods.stats_method import StatsCollectionMethod
from src.status import MainStatus, MonthDatasetStatus
from src.util import year_month_str


def reset() -> None:
    if not CONFIG.TEST_MODE:
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


def iter_dumps_main(settings: IterationSettings, month_ds_status: Optional[MonthDatasetStatus],
                    methods: list[IterationMethod]):
    # complex_main_generic_all_data(settings, month_ds_status, [PostFilterMethod,
    #                                                           MediaFilterMethod,
    #                                                           StatsCollectionMethod,
    #                                                           IndexEntriesDB,
    #                                                           AnnotationDBMethod])

    base_month_data_iterator(settings, month_ds_status, methods)


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
    settings = IterationSettings(CONFIG.YEAR, CONFIG.MONTH, CONFIG.LANGUAGES, CONFIG.ANNOT_EXTRA)
    month_status = main_status.year_months[ym_s]

    filter_method = MethodDefinition(
        method_name=str(PostFilterMethod.name),
        method_type=PostFilterMethod,
        config = {})

    collect_hashtags_method = MethodDefinition(
        method_name=str(StatsCollectionMethod.name),
        method_type=StatsCollectionMethod,
        config={"collect_hashtags": True}
    )

    annotation_db_method = MethodDefinition(
        method_name=str(AnnotationDBMethod.name),
        method_type=AnnotationDBMethod,
        config=AnnotationDBMethodConfig(skip_minutes=3)
    )

    repack_method = MethodDefinition(method_name=str(PackEntries.name),
                                     method_type=PackEntries,
                                     config={"delete_jsonl_files": True, "gzip_files": True})

    methods = create_methods(settings,
                             [filter_method, repack_method])
    # main process going through the dump folder

    if CONFIG.TEST_MODE:
        logger.info("Test-mode on")

    # CHECK ITER SOURCE

    if CONFIG.DATA_SOURCE == DATA_SOURCE_DUMP:
        iter_dumps_main(settings, month_status, methods)
    elif CONFIG.DATA_SOURCE == DATA_SOURCE_REPACK:
        repack_iterator(settings, month_status, methods)
    else:
        logger.error(f"unknown data-source: {CONFIG.DATA_SOURCE}")
    # checking label-studio project
    # if not month_status.label_studio_project_ids:
    #     ls_client = LabelStudioManager()
    #     month_status.label_studio_project_ids = ls_client.create_projects_for_db("twitter",
    #                                                                              settings,
    #                                                                              CONFIG.LABELSTUDIO_LABEL_CONFIG_FILENAME)
    #
    # main_status.print_database_status(month_status)
    if main_status:
        main_status.store_status()


if __name__ == '__main__':
    main()
