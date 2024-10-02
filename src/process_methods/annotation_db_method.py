from typing import Any

from src.consts import METHOD_ANNOTATION_DB, locationindex_type, METHOD_FILTER
from src.models import IterationSettings, AnnotPostCollection
from src.mutli_func_iter import IterationMethod
from src.status import MonthDatasetStatus


class AnnotationDBMethod(IterationMethod):

    def __init__(self, settings: IterationSettings):
        super().__init__(settings)

        self.post_collection = AnnotPostCollection(settings.languages,
                                                   settings.year,
                                                   settings.month,
                                                   settings.annotation_extra)

    @property
    def name(self) -> str:
        return METHOD_ANNOTATION_DB

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        self.post_collection.add_post(post_data, location_index)

    def finalize(self):
        self.post_collection.validate()
        self.post_collection.finalize_dbs()

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        status.annotated_db_available = True
