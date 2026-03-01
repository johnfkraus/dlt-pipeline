# silver_c01_polars.py

info = """
spreadsheet columns > silver
selector_a
selector_b
name_a
name_b
date_of_interaction
additional_info
comment
dataset_name

"""

import psycopg2
import polars as pl
import tomllib  # Python 3.11+; use `tomli` for older versions
from pathlib import Path
from timer import get_time

from common_utils import (
    normalize_date,
    normalize_name,
    normalize_phone_cn,
    normalize_phone_cn_target,
    normalize_empty,
    get_latest_schema_containing_named_table
)

BRONZE_SCHEMA_PREFIX = "bronze"
SILVER_SCHEMA = "silver"
TABLE_NAME = "c02"
NONE = "None"

def load_db_params_from_secrets(
        path: str = ".dlt/secrets.toml",
        section: str = "destination.postgres.credentials",
) -> dict:
    """
    Read Postgres connection parameters from .dlt/secrets.toml.
    Expects either discrete fields (database, username, password, host, port)
    or a DSN string.
    """
    secrets_path = Path(path)
    if not secrets_path.exists():
        raise FileNotFoundError(f"Secrets file not found at {path}")

    with secrets_path.open("rb") as f:
        cfg = tomllib.load(f)

    # Nested keys are represented as nested tables in TOML
    dest_cfg = cfg
    for part in section.split("."):
        dest_cfg = dest_cfg.get(part, {})

    if not dest_cfg:
        raise KeyError(f"Section [{section}] not found in {path}")

    # DSN style (optional)
    dsn = dest_cfg.get("dsn")
    if dsn:
        return {"dsn": dsn}

    # Discrete fields style
    return {
        "dbname": dest_cfg.get("database"),
        "user": dest_cfg.get("username"),
        "password": dest_cfg.get("password"),
        "host": dest_cfg.get("host", "localhost"),
        "port": dest_cfg.get("port", 5432),
    }


@get_time
def main():
    conn_params = load_db_params_from_secrets()

    # Use DSN if provided, otherwise keyword params
    if "dsn" in conn_params:
        conn = psycopg2.connect(conn_params["dsn"])
    else:
        conn = psycopg2.connect(**conn_params)

    with conn:

        BRONZE_SCHEMA = get_latest_schema_containing_named_table(conn, BRONZE_SCHEMA_PREFIX, TABLE_NAME)
        print(f"BRONZE_SCHEMA: {BRONZE_SCHEMA}")
        # conn.autocommit = True

        # 1) Load bronze data into Polars
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
                FROM {BRONZE_SCHEMA}.{TABLE_NAME};
                """
            )
            rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]

        df = pl.DataFrame(rows, schema=col_names, orient="row")
        print("bronze:")
        print(df.head())
        # Fill nulls in column "b" with 0
        # df = df.with_columns(
        #     pl.col("contact").fill_null("None"),
        #     pl.col("target").fill_null("None")
        # )

        # 2) Transform with Polars
        df = df.with_columns(
            [
                pl.col("selector_a").fill_null(NONE).map_elements(normalize_phone_cn,  return_dtype=pl.Utf8),
                pl.col("selector_b").fill_null(NONE).map_elements(normalize_phone_cn,  return_dtype=pl.Utf8),
                pl.col("name_a").fill_null(NONE).map_elements(normalize_name,  return_dtype=pl.Utf8),
                pl.col("name_b").fill_null(NONE).map_elements(normalize_name,  return_dtype=pl.Utf8),
                pl.col("date_of_interaction").fill_null(NONE).map_elements(normalize_date,  return_dtype=pl.Utf8),
                # pl.col("date_of_last_interaction").fill_null(NONE).map_elements(normalize_date,  return_dtype=pl.Utf8),
                pl.col("additional_info").fill_null(NONE).map_elements(normalize_empty,  return_dtype=pl.Utf8),
                pl.col("comment").fill_null(NONE).map_elements(normalize_empty, return_dtype=pl.Utf8),
                pl.col("dataset_name").fill_null(NONE).map_elements(normalize_empty,  return_dtype=pl.Utf8),
            ]
        )
        print("normalized df:")
        print(df.head())

        # 3) Create silver table and bulk-insert
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA};")

            cur.execute(f"DROP TABLE IF EXISTS {SILVER_SCHEMA}.{TABLE_NAME};")
            cur.execute(
                f"""
                CREATE TABLE {SILVER_SCHEMA}.{TABLE_NAME} (
                    _dlt_id TEXT,
                    _dlt_load_id TEXT,
                    selector_a TEXT,
                    selector_b TEXT,
                    name_a TEXT,
                    name_b TEXT,
                    date_of_interaction TEXT,
                    additional_info TEXT,
                    comment TEXT,
                    dataset_name TEXT
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
                    "additional_info",
                    "comment",
                    "dataset_name",
                ]
            ).rows()  # returns list[tuple]  # to_tuples()

            insert_sql = f"""
                INSERT INTO {SILVER_SCHEMA}.{TABLE_NAME} (
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
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """

            cur.executemany(insert_sql, records)


if __name__ == "__main__":
    main()
