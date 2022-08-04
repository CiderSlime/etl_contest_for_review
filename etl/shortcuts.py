import pymysql.cursors as pc
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
