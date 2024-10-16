import json
from typing import Optional, Any

from src.consts import CONFIG, MAIN_STATUS_FILE_PATH, BASE_DBS_PATH, BASE_STAT_PATH, logger, BASE_DATA_PATH, \
    DATA_SOURCE_DUMP, DATA_SOURCE_REPACK, BASE_METHODS_CONFIG_PATH, PROJECT_PATH
from src.data_iterators.base_data_iterator import base_month_data_iterator
from src.data_iterators.repacked_data_iterator import repack_iterator
from src.models import MethodDefinition, IterationSettings
from src.process_methods.abstract_method import IterationMethod, create_methods
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


def init_methods():
    # init/load methods config
    config_file = BASE_METHODS_CONFIG_PATH / CONFIG.METHODS_CONFIG_FILE
    methods_config: dict[str, dict[str, Any]] = {}
    if not config_file.exists():
        print(f"methods config file not found: {config_file.resolve(PROJECT_PATH)}. USING DEFAULTS")
    else:
        methods_config = json.load(config_file.open())

    all_methods = {}

    # Filter method

    from src.process_methods.post_filter_method import PostFilterMethod
    filter_name = PostFilterMethod.name()
    print(f"filter config defined: {filter_name in methods_config}")
    all_methods[filter_name] = MethodDefinition(
        method_name=filter_name,
        method_type=PostFilterMethod,
        config=methods_config.get(filter_name, {}))

    # Stats collection method

    from src.process_methods.stats_method import StatsCollectionMethod

    stats_name = StatsCollectionMethod.name()
    print(f"stats config defined: {stats_name in methods_config}")
    all_methods[stats_name] = MethodDefinition(
        method_name=stats_name,
        method_type=StatsCollectionMethod,
        config=methods_config.get(stats_name, {})
    )

    # Annotation DB ,

    from src.process_methods.annotation_db_method import AnnotationDBMethod

    annotation_db_name = AnnotationDBMethod.name()
    print(f"Annotation-DS creation config defined: {annotation_db_name in methods_config}")
    all_methods[annotation_db_name] = MethodDefinition(
        method_name=annotation_db_name,
        method_type=AnnotationDBMethod,
        config=methods_config.get(annotation_db_name, {})
    )

    # Repack Method

    from src.process_methods.repack_data import RepackEntriesMethod

    repacke_name = RepackEntriesMethod.name()
    print(f"Repack config defined: {repacke_name in methods_config}")
    all_methods[repacke_name] = MethodDefinition(method_name=repacke_name,
                                                 method_type=RepackEntriesMethod,
                                                 config=methods_config.get(repacke_name, {}))

    try:
        from src.process_methods.auto_relecanve_check_method import AutoRelevanceMethod

        autorelevance_name = AutoRelevanceMethod.name()
        all_methods[autorelevance_name] = MethodDefinition(
            method_name=autorelevance_name,
            method_type=AutoRelevanceMethod,
            config=methods_config.get(autorelevance_name, {})
        )
    except ImportError:
        print(f"import failed for AutoRelevanceMethod. Method not usable")

    try:
        from src.process_methods.simple_waether_bot_filter import SimpleWeatherBotFilter
        from src.process_methods.simple_waether_bot_filter import WeatherBotFilter

        simple_weather_bot_filter_name = SimpleWeatherBotFilter.name()
        all_methods[simple_weather_bot_filter_name] = MethodDefinition(
            method_name=simple_weather_bot_filter_name,
            method_type=SimpleWeatherBotFilter,
            config=WeatherBotFilter(
                bot_vectors_file=BASE_DATA_PATH / "temp/bot_detection/bot_embeddings.json",
                human_vectors_file=BASE_DATA_PATH / "temp/bot_detection/human_embeddings.json"
            )
        )
    except ImportError:
        print(f"import failed for SimpleWeatherBotFilter. Method not usable")

    selected_methods = []

    for m in CONFIG.METHODS:
        if m not in all_methods:
            print(f"Method '{m}' not defined.")
        else:
            selected_methods.append(m)

    return selected_methods


def data_process_main():
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

    selected_methods = init_methods()

    if CONFIG.CONFIRM_RUN:
        print(f"data source: {CONFIG.DATA_SOURCE}")
        print(f"test mode: {CONFIG.TEST_MODE}")
        print(f"languages: {CONFIG.LANGUAGES}")
        print(f"year month: {CONFIG.YEAR}-{CONFIG.MONTH}")
        print(f"methods: {[m.method_name for m in selected_methods]}")
        input("press any key to continue")

    methods = create_methods(settings, selected_methods)

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

    # repack_iter = RandomPackedDataIterator(settings, month_status, methods)
    # entries = []
    # entry_ids = set()
    # limit = 1000
    # for a in tqdm(repack_iter, total=limit):
    #     if not a:
    #         continue
    #     path, index, entry, random_index = a
    #     # print(path, index, random_index)
    #     # print(a)
    #     if a:
    #         if entry["id"] in entry_ids:
    #             continue
    #         # print(len(entries))
    #         entry_ids.add(entry["id"])
    #         entries.append(a)
    #         if len(entries) > limit:
    #             break
    #     else:
    #         print("something strange happened, no entry")
    # json.dump(entries, (BASE_DATA_PATH / "test_dump").open("w", encoding="utf-8"), ensure_ascii=False)
    #
    # create_nature4axis_tasks([e[2] for e in entries], "test_tasks")

    # if main_status:
    #     main_status.store_status()


def main() -> None:
    data_process_main()

    pass
    # checking label-studio project
    # if not month_status.label_studio_project_ids:
    #     ls_client = LabelStudioManager()
    #     month_status.label_studio_project_ids = ls_client.create_projects_for_db("twitter",
    #                                                                              settings,
    #                                                                              CONFIG.LABELSTUDIO_LABEL_CONFIG_FILENAME)
    #
    # main_status.print_database_status(month_status)


if __name__ == '__main__':
    main()
    # fp = BASE_REPACK_PATH / "2022-02/20220201/en/202202010045.jsonl.gz"
    # file_data = read_gzip_file(fp)
    # print(file_data)
    # print(read_gzip_file_and_count_lines(fp))
    # for aas in iter_jsonl_data2(fp):
    #     print(aas)
