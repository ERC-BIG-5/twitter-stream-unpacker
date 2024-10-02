import json
from collections.abc import Iterable
from typing import Optional

import deepdiff
import genson
from genson import SchemaBuilder
from sqlalchemy import func, select







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