# gold/gold_mart_polars.py
# https://docs.pola.rs/user-guide/getting-started/#concatenating-dataframes
import polars as pl
import datetime as dt
import adbc_driver_postgresql.dbapi as pg_adbc  # pip install adbc-driver-postgresql


# df = pl.read_database(
#     "SELECT id, field_a, field_b, other_col FROM public.source_table",
#     conn,
# )
import psycopg2
import tomllib  # Python 3.11+; use `tomli` for older versions
from psycopg2.extras import Json  # helps adapt dict → jsonb [web:207][web:212]
from pathlib import Path
from timer import get_time

from common_utils import (
    load_db_params_from_secrets
)

# SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
TABLE_NAMES = ["c01", "c02", "c03"]
MART_TABLE_NAME = "comms_mart"

@get_time
def main():
    conn_params = load_db_params_from_secrets()
    uri = f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/comms"

    conn = pg_adbc.connect(uri)  # "postgresql://user:password@localhost:5432/your_db")

    # Use DSN if provided, otherwise keyword params
    # if "dsn" in conn_params:
    #     conn = psycopg2.connect(conn_params["dsn"])
    # else:
    #     conn = psycopg2.connect(**conn_params)

    with conn:
        list_of_dfs = []
        for table_name in TABLE_NAMES:
            # 1) Load temp gold tables into Polars; source table
            df = pl.read_database(f"SELECT * FROM {GOLD_SCHEMA}.{table_name};", conn)

            list_of_dfs.append(df)

            # pl.Config.set_fmt_str_lengths(200)  # Shows up to 100 characters
            # print(df['other_info'])

        df_mart = pl.concat(list_of_dfs, how="vertical")

        # print(df_mart)

        df_mart.write_database(
            table_name=f"{GOLD_SCHEMA}.{MART_TABLE_NAME}",
            connection=conn,  # "postgresql://user:password@host:port/database",
            engine="adbc",
            if_table_exists="replace"  # or "append", "fail"
        )
        # inspect the finished mart
        df_mart_check = pl.read_database(
            f"SELECT * FROM {GOLD_SCHEMA}.{MART_TABLE_NAME}",
            conn,
        )
        print("comms mart final table:")
        print(df_mart_check)

        # print(df.head())
        # pl.Config.set_fmt_str_lengths(200)  # Shows up to 100 characters


if __name__ == "__main__":
    main()






# print(pl.concat([TABLE_NAMES], how="vertical"))
