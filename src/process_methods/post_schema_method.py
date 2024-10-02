import json
from typing import Optional

import deepdiff
import genson

from src.consts import METHOD_SCHEMA, locationindex_type, METHOD_FILTER
from src.models import IterationSettings
from src.process_methods.abstract_method import IterationMethod


class EntrySchema(IterationMethod):

    @property
    def name(self) -> str:
        return METHOD_SCHEMA

    def __init__(self, settings: IterationSettings):
        super().__init__(settings)
        self.collect_num_posts = 50
        self.create_schema_from: list[dict] = []


    def _process_data(self, post_data: dict, location_index: locationindex_type):
        self.create_schema_from.append(post_data)
        if len(self.create_schema_from) == self.collect_num_posts:
            self._build_schema()

    def _build_schema(objects: list[dict], check_div_every_k: Optional[int] = None) -> dict:
        """
        note: this seems to be the way to avoid that some props are removed.

        :param objects:
        :param check_div_every_k:
        :return:
        """
        builder = genson.SchemaBuilder()
        cur_schema = {}
        for idx, obj in enumerate(objects):
            cur_builder = genson.SchemaBuilder()
            cur_builder.add_object(obj)
            builder.add_schema(cur_builder)
            if idx == 0:
                cur_schema = builder.to_schema()
            if check_div_every_k is not None and (idx != 0 and idx % check_div_every_k == 0):
                next_schema = builder.to_schema()
                diff = deepdiff.DeepDiff(cur_schema, next_schema)
                print(json.dumps(json.loads(diff.to_json()), indent=2))
                # print(json.dumps(diff.to_dict(), indent=2))

        return cur_schema
