import json
from collections.abc import Iterable
from typing import Optional

import deepdiff
import genson
from genson import SchemaBuilder
from sqlalchemy import func, select


def build_schema(objects: Iterable[dict], check_div_every_k: Optional[int] = None) -> dict:
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
        # if idx == 0:
        #     cur_schema = builder.to_schema()
        # if check_div_every_k is not None and (idx != 0 and idx % check_div_every_k == 0):
        #     next_schema = builder.to_schema()
        #     diff = deepdiff.DeepDiff(cur_schema, next_schema)
        #     print(json.dumps(json.loads(diff.to_json()), indent=2))
            #print(json.dumps(diff.to_dict(), indent=2))
    return builder.to_schema()


def test_basic():
    # Example usage
    obj1 = {
        "name":"x",
        "age":12
    }

    obj2 = {
        "name":"x",
        "style":"cool"
    }


    #schema = build_schema([obj1, obj2], 1)
    #print(json.dumps(schema, indent=2))

    schema1 = SchemaBuilder()
    schema1.add_object(obj1)
    schema2 = SchemaBuilder()
    schema2.add_object(obj2)
    schema1.add_schema(schema2)
    print(json.dumps(schema1.to_schema(), indent=2))


if __name__ == '__main__':
    #test_()
    test_basic()