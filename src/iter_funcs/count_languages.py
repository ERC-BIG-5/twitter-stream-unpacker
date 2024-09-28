from src.consts import locationindex_type
from src.simple_generic_iter import main_generic_all_data


def collect_languages():
    languages: set[str] = set()

    def collect_lang(data: dict, location_index: locationindex_type):
        languages.add(data["lang"])

    main_generic_all_data(collect_lang)
    print(languages)

