import pymysql
import typing as t
from datetime import timedelta, datetime

from etl.shortcuts import get, getall, write_row


class BatchIterator:
    """
    Проходим часовыми батчами начиная с минимального dt, либо с последнего найденного в dest.
    Завершаем итерацию когда доходим до максимального dt.

    """
    def __init__(self, src_cursor, dst_cursor):
        self.dt_start = None
        self.dt_max = None
        self.is_proceeding = False  # флаг того что мы возобновляем мердж в непустую таблицу
        self.src_cursor = src_cursor
        self.dst_cursor = dst_cursor

    def __iter__(self):
        self.dt_start = get(self.dst_cursor, "SELECT MAX(dt) FROM transactions_denormalized")[0]

        if self.dt_start:
            self.is_proceeding = True

        else:
            self.dt_start = get(self.src_cursor, "SELECT MIN(dt) FROM transactions")[0]

        self.dt_max = get(self.src_cursor, "SELECT MAX(dt) FROM transactions")[0]
        return self

    def __next__(self):
        if self.dt_start > self.dt_max:
            raise StopIteration

        next_dt_start = self.dt_start + timedelta(hours=1)

        rows = getall(
            self.src_cursor,
            """
                SELECT  t.*, ot.name from transactions t JOIN operation_types ot ON t.idoper = ot.id 
                WHERE dt>= timestamp('%s') AND dt< timestamp('%s')
            """ % (
                self.dt_start, next_dt_start
            )
        )

        self.dt_start = next_dt_start
        return rows

    def merge_rows_to_destination(self, transactions: t.List[tuple]):
        for transaction in transactions:

            # при возобновлении можем повторно начать записывать строки с одинаковым временем, нужно проверить их в dest
            if self.is_proceeding:
                if get(self.dst_cursor, "SELECT id FROM transactions_denormalized where id=%s" % transaction[0])[0]:
                    continue

            write_values = (
                transaction[0],
                transaction[1],
                transaction[2],
                transaction[3],
                str(transaction[4]),
                transaction[5]
            )
            write_row(self.dst_cursor, write_values)

        # дубликаты возможны только в первом батче, в дальнейшем проверять на них не требуется
        if self.is_proceeding:
            self.is_proceeding = False


def merge_tables(mysql_src_credentials, mysql_dst_credentials):
    with pymysql.connect(**mysql_src_credentials) as src_conn, pymysql.connect(**mysql_dst_credentials, autocommit=True) as dst_conn:
        with src_conn.cursor() as src_cur, dst_conn.cursor() as dst_cur:
            iterator = BatchIterator(src_cur, dst_cur)
            for rows_batch in iterator:
                iterator.merge_rows_to_destination(rows_batch)

            return  # to place breakpoint
