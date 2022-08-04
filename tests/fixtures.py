import datetime

import pytest
import pathlib
import pymysql

from etl.shortcuts import write_row
from .helpers import (
    load_json_config,
    teardown_database_schema,
    setup_database_schema,
    setup_schema_data
)

from .schema_src import (
    source_setup_ddls,
    source_teardown_ddls,
    source_setup_data
)

from .schema_dst import (
    destination_setup_ddls,
    destination_teardown_ddls,
    destination_setup_data
)


here = pathlib.Path(__file__).resolve()
config_path = here.parents[1] / "dbconf.json"


@pytest.fixture(scope="session")
def databases_config():
    return load_json_config(config_path)


@pytest.fixture(scope="session")
def mysql_src_credentials(databases_config):
    return databases_config["mysql_src"]


@pytest.fixture(scope="session")
def mysql_dst_credentials(databases_config):
    return databases_config["mysql_dst"]


@pytest.fixture()
def mysql_source(mysql_src_credentials):

    with pymysql.connect(**mysql_src_credentials, autocommit=True) as conn:
        with conn.cursor() as cur:
            teardown_database_schema(cur, source_teardown_ddls)
            setup_database_schema(cur, source_setup_ddls)
            setup_schema_data(cur, source_setup_data)

    return mysql_src_credentials


@pytest.fixture()
def mysql_destination(mysql_dst_credentials):

    with pymysql.connect(**mysql_dst_credentials, autocommit=True) as conn:
        with conn.cursor() as cur:
            teardown_database_schema(cur, destination_teardown_ddls)
            setup_database_schema(cur, destination_setup_ddls)
            setup_schema_data(cur, destination_setup_data)

    return mysql_dst_credentials


@pytest.fixture()
def mysql_destination_with_partial_result(mysql_destination):

    with pymysql.connect(**mysql_destination, autocommit=True) as conn:
        with conn.cursor() as cur:

            write_row(cur, (1, datetime.datetime(2020, 1, 1, 0, 0, 0), 1, -1, 100, "Subscription purchase"))
            write_row(cur, (2, datetime.datetime(2020, 1, 1, 1, 0, 0), 1, -1, 100, "Subscription purchase"))
            write_row(cur, (3, datetime.datetime(2020, 1, 1, 2, 0, 0), 2, -1, 100, "Subscription update"))
            write_row(cur, (4, datetime.datetime(2020, 1, 1, 2, 0, 0), 2, -1, 100, "Subscription update"))

    return mysql_destination
