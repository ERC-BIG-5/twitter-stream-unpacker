import json
import sys
from random import random

from src.consts import locationindex_type
from src.json_schema_builder import build_schema
from src.simple_generic_iter import main_generic_all_data


def generic_iter_collect_schema():
    """
    collect the schema over 100 tweets. semi-randomly selected
    :return:
    """

    def generate_json_schema():

        pick = []

        def pick_for_schema(data: dict, location_index: locationindex_type):
            if random() < 0.00005:
                pick.append(data)
                print(len(pick))
            if len(pick) == 100:
                schema = build_schema(pick)
                print(json.dumps(schema, indent=2))
                sys.exit()

        return pick_for_schema

    main_generic_all_data(generate_json_schema())

