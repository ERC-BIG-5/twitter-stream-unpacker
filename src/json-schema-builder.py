import json
from collections.abc import Iterable

import deepdiff
import genson
from genson import SchemaBuilder
from sqlalchemy import func, select

from db import init_db, DBPost, annotation_db_path


def build_schema(objects: Iterable[dict], check_div_every_k: int) -> dict:
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
        if idx != 0 and idx % check_div_every_k == 0:
            next_schema = builder.to_schema()
            diff = deepdiff.DeepDiff(cur_schema, next_schema)
            print(json.dumps(json.loads(diff.to_json()), indent=2))
            #print(json.dumps(diff.to_dict(), indent=2))

    return cur_schema


def test_(k: int = 10):
    with init_db(annotation_db_path(3))() as session:
        # user_count = session.query(func.count(DBPost.id)).scalar()
        # print(f"Total number of posts in the database: {user_count}")
        query = select(DBPost.id, DBPost.content).order_by(func.random()).limit(k)
        posts = session.execute(query)
        # for id, post in posts:
        # print(post)
        schema = build_schema((p[1] for p in posts), 2)
        print(json.dumps(schema, indent=2))

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