import json
from datetime import datetime
from typing import Optional, Union, Sequence

from pydantic import BaseModel

from src.consts import locationindex_type, METHOD_FIND, BASE_DATA_PATH, get_logger
from src.models import IterationSettings
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus

logger = get_logger(__file__)

class FindConfig(BaseModel):
    find_ids: list[int]


class FindMethod(IterationMethod):
    """
    just until we dont have an index. find tweets by their id
    """

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        pass

    @staticmethod
    def name() -> str:
        return METHOD_FIND

    def __init__(self, settings: IterationSettings, config: Optional[Union[BaseModel, dict]]):
        super().__init__(settings, config)
        self.config = FindConfig.model_validate(config)
        self.find_ids = set(self.config.find_ids)
        self.found: dict[int, tuple[Sequence, dict]] = {}

        self.dest_path = BASE_DATA_PATH / f"temp/find/{settings.month}-{datetime.now().strftime('%Y%m%d-%H%M')}.json"
        self.dest_path.parent.mkdir(exist_ok=True)

    def _process_data(self, post_data: dict, location_index: locationindex_type):
        if (id := post_data["id"]) in self.find_ids:
            if id not in self.found:
                self.found[id] = location_index, post_data
                logger.info(f"found {id}")
        return

    def finalize(self):
        missing = set(self.find_ids) - set(self.found.keys())

        json.dump(
            {
                "missing": list(missing),
                "items": self.found
            }, self.dest_path.open("w", encoding="utf-8"), ensure_ascii=False)

    def print_outputs(self):
        print(f"dumping found tweets to {self.dest_path}")
