from src.consts import locationindex_type
from src.mutli_func_iter import IterationMethod, IterationSettings
from src.simple_generic_iter import main_generic_all_data


def collect_languages():
    languages: set[str] = set()

    def collect_lang(data: dict, location_index: locationindex_type):
        languages.add(data["lang"])

    main_generic_all_data(collect_lang)
    print(languages)


class CollectLanguagesMethod(IterationMethod):

    def __init__(self, settings: IterationSettings):
        super().__init__(settings)
        self.languages: set[str] = set()

    def process_data(self, post_data: dict, location_index: locationindex_type):
        self.languages.add(post_data["lang"])

    def finalize(self):
        print(self.languages)
