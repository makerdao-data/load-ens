import snowflake
import snowflake.connector
from random import randint
import os, sys


def _write_to_stage(conn, records, stage):

    file_name = None
    if records:

        file_name = 'dump_' + str(randint(1, 999999999)).zfill(9) + '.csv'

        csv_file = open(file_name, 'w')
        for record in records:
            csv_file.write('%s\n' % '|'.join(
                [attribute.__str__().replace('|', '')
                 for attribute in record]))
        csv_file.close()

        try:
            conn.execute("PUT file://%s @%s" % (file_name, stage))
            # if os.path.exists(file_name):
            #     os.remove(file_name)
        except snowflake.connector.errors.ProgrammingError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate,
                                                      e.msg, e.sfqid))
            # if os.path.exists(file_name):
            #     os.remove(file_name)


    return file_name


def _clear_stage(conn, stage, pattern):
    conn.execute(f"""REMOVE @{stage}/{pattern}""")


def _write_to_table(conn, stage, table, pattern, purge=True):

    conn.execute(
        f"""COPY INTO {table} FROM @{stage}/{pattern}.gz FILE_FORMAT=(TYPE=CSV, FIELD_DELIMITER='|', NULL_IF='None') PURGE={purge}; """
    )

    return True
