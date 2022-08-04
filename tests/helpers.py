import json
import typing as t
import pathlib
import pymysql.cursors as pc


def load_json_config(config_path: pathlib.Path) -> dict:
    with open(config_path, "r") as f:
        return json.load(f)


def teardown_database_schema(cursor: pc.Cursor, queries: t.List[str]):
    for query in queries:
        cursor.execute(query)


def setup_database_schema(cursor: pc.Cursor, queries: t.List[str]):
    for query in queries:
        cursor.execute(query)


def setup_schema_data(cursor: pc.Cursor, queries: t.List[t.List]):
    for query, params in queries:
        cursor.executemany(query, params)


