import pymysql.cursors as pc
import pymysql as pm
import typing as t


def get(cursor: pc.Cursor, query: str) -> tuple:
    cursor.execute(query)
    return cursor.fetchone()


def getall(cursor: pc.Cursor, query: str) -> t.List[tuple]:
    cursor.execute(query)
    return cursor.fetchall()


def write_row(dst_cursor: pc.Cursor, row: tuple):
    write_statement = "INSERT INTO transactions_denormalized VALUES (%s, timestamp('%s'), %s, %s, %s, '%s')" \
                      % row
    dst_cursor.execute(write_statement)


def check_etl_result_relevance(mysql_source, mysql_destination):
    with pm.connect(**mysql_source) as src_conn, pm.connect(**mysql_destination) as dst_conn:
        with src_conn.cursor() as src_cur, dst_conn.cursor() as dst_cur:

            for transaction in getall(
                    src_cur,
                    "SELECT t.*, ot.name from transactions t JOIN operation_types ot ON t.idoper = ot.id"
            ):
                result_row = get(dst_cur, "SELECT * FROM transactions_denormalized WHERE id=%s" % transaction[0])

                assert result_row == transaction
