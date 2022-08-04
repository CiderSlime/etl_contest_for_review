import json
import typing as t
import pathlib
import pymysql as pm
import pymysql.cursors as pc

from etl.shortcuts import getall, get


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


def check_etl_result_relevance(mysql_source, mysql_destination):
    with pm.connect(**mysql_source) as src_conn, pm.connect(**mysql_destination) as dst_conn:
        with src_conn.cursor() as src_cur, dst_conn.cursor() as dst_cur:

            for transaction in getall(
                    src_cur,
                    "SELECT t.*, ot.name from transactions t JOIN operation_types ot ON t.idoper = ot.id"
            ):
                result_row = get(dst_cur, "SELECT * FROM transactions_denormalized WHERE id=%s" % transaction[0])

                assert result_row[1] == transaction[1]  # dt
                assert result_row[2] == transaction[2]  # idoper
                assert result_row[3] == transaction[3]  # move
                assert result_row[4] == transaction[4]  # amount
                assert result_row[5] == transaction[5]  # name


