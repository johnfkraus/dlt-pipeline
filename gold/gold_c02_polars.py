# gold/gold_c02_polars.py
info = """
table co2 silver columns
------
_dlt_id,
_dlt_load_id,
selector_a,
selector_b,
name_a,
name_b,
date_of_interaction,
additional_info,
comment,
dataset_name
"""

import psycopg2
import json
import polars as pl
import tomllib  # Python 3.11+; use `tomli` for older versions
from psycopg2.extras import Json  # helps adapt dict → jsonb [web:207][web:212]
from pathlib import Path
from timer import get_time


from common_utils import (
    # normalize_date,
    # normalize_name,
    # normalize_phone_cn,
    # normalize_empty,
    load_db_params_from_secrets
)

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
TABLE_NAME = "c02"
NONE = "None"


@get_time
def main():
    conn_params = load_db_params_from_secrets()

    # Use DSN if provided, otherwise keyword params
    if "dsn" in conn_params:
        conn = psycopg2.connect(conn_params["dsn"])
    else:
        conn = psycopg2.connect(**conn_params)
    with conn:
        # conn.autocommit = True
        # 1) Load silver data into Polars; source table
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    _dlt_id,
                    _dlt_load_id,
                    selector_a,
                    selector_b,
                    name_a,
                    name_b,
                    date_of_interaction,
                    additional_info,
                    comment,
                    dataset_name
                FROM {SILVER_SCHEMA}.{TABLE_NAME};
                """
            )
            rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]

        df = pl.DataFrame(rows, schema=col_names, orient="row")
        print("silver data df.head():")
        print(df.head())

        # other = {
        #     "additional_info": df['additional_info'],
        #     "dataset_name": df['dataset_name'],  # dataset,
        #     # "comment": df['comment']  #  comment,
        # }
        # print(f"{other=}, {type(other)=}")
        # other_json = json.dumps(other)
        # print(f">>>>>> {other_json=}")

        # 2) Transform with Polars
        df = df.with_columns(
            [
                pl.struct(["additional_info", "dataset_name"]).alias("other_info"),
                # pl.col("contact").alias("selector_a"),
                # pl.col("target").alias("selector_b"),
                # pl.col("person_a").alias("name_a"),
                # pl.col("person_b").alias("name_b"),
                # pl.col("date_of_first_interaction").alias("date_of_interaction"),
                # pl.col("date_of_last_interaction").fill_null(NONE).map_elements(normalize_date,  return_dtype=pl.Utf8),
                # pl.col("additional_info").fill_null(NONE).map_elements(normalize_empty, return_dtype=pl.Utf8),
                # pl.col("dataset_name").fill_null(NONE).map_elements(normalize_empty, return_dtype=pl.Utf8),
            ]
        )
        print("gold transformed df")
        print(df.columns)
        print(df.head())
        print("99: ", df['other_info'])

        # 3) Create gold (target) table and bulk-insert
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA};")

            cur.execute(f"DROP TABLE IF EXISTS {GOLD_SCHEMA}.{TABLE_NAME};")
            cur.execute(
                f"""
                CREATE TABLE {GOLD_SCHEMA}.{TABLE_NAME} (
                    _dlt_id TEXT,
                    _dlt_load_id TEXT,
                    selector_a TEXT,
                    selector_b TEXT,
                    name_a TEXT,
                    name_b TEXT,
                    date_of_interaction TEXT,
                    -- additional_info TEXT,
                    other_info TEXT
                );
                """
            )

            records = df.select(
                [
                    "_dlt_id",
                    "_dlt_load_id",
                    "selector_a",
                    "selector_b",
                    "name_a",
                    "name_b",
                    "date_of_interaction",
                    # "additional_info",
                    "other_info",
                    "dataset_name",
                ]
            ).rows(named=True)  # returns list[tuple]  # to_tuples()
            # ).rows()  # returns list[tuple]  # to_tuples()

            insert_sql = f"""
                INSERT INTO {GOLD_SCHEMA}.{TABLE_NAME} (
                    _dlt_id,
                    _dlt_load_id,
                    selector_a,
                    selector_b,
                    name_a,
                    name_b,
                    date_of_interaction,
--                     additional_info,
                    other_info
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
            """
            # generate the VALUES for above:
            for rec in records:
                print(f"{rec=}")
                other_info_json_dict = {
                    "additional_info": rec["other_info"]["additional_info"],
                    # "contact": rec["other_info"]["contact"],
                    # "target": rec["other_info"]["target"],
                    # "date_of_first_interaction": rec["other_info"]["date_of_first_interaction"],
                    # "date_of_last_interaction": rec["other_info"]["date_of_last_interaction"],
                    "dataset_name": rec["other_info"]["dataset_name"],
                }
                print(f"{other_info_json_dict=}")
                cur.execute(
                    insert_sql,  # records)
                    (
                        rec["_dlt_id"],
                        rec["_dlt_load_id"],
                        rec["selector_a"],
                        rec["selector_b"],
                        rec["name_a"],
                        rec["name_b"],
                        rec["date_of_interaction"],
                        # rec["additional_info"],
                        Json(other_info_json_dict),
                        # rec["dataset_name"]
                    ),
                )

        # cur.executemany(insert_sql, records)
            # Inspect the temp gold table
            cur.execute(
                f"""
                SELECT * 
                FROM {GOLD_SCHEMA}.{TABLE_NAME};
                """
            )
            rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]

        df = pl.DataFrame(rows, schema=col_names, orient="row")
        print("gold data df.head():")
        print(df.head())
        pl.Config.set_fmt_str_lengths(200)  # Shows up to 100 characters

        print(df['other_info'])


if __name__ == "__main__":
    main()
