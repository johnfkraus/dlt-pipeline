# print_db_adbc.py
import polars as pl
import adbc_driver_postgresql.dbapi as pg_adbc  # pip install adbc-driver-postgresql

from timer import get_time

from common_utils import (
    load_db_params_from_secrets
)

@get_time
def print_db(database_name, schema_name, table_name):
    """
    Print a database table to the console.

    :param database_name:
    :param schema_name:
    :param table_name:
    :return:
    """
    conn_params = load_db_params_from_secrets()
    uri = f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{database_name}"

    conn = pg_adbc.connect(uri)

    with conn:

        # inspect the finished mart
        df = pl.read_database(
            f"SELECT * FROM {schema_name}.{table_name}",
            conn,
        )

        print(df)


if __name__ == "__main__":
    print_db("comms", "gold", "comms_mart")

