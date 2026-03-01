<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Can  I use something like polars to make this code run faster when the dataset is large?  If so, please show me the python code.

Yes, you can use Polars to speed up the CSV → Postgres part, especially for large files. Polars is typically faster and more memory‑efficient than pandas for big datasets and heavy transformations.[^1][^2][^3][^4][^5][^6]

Below is an example `silver_co1.py` rewritten to use Polars for all row‑wise transformations, then batch inserts into Postgres.

***

## silver_co1_polars.py (context manager + Polars)

```python
# silver_co1_polars.py
import psycopg2
import polars as pl

from common_utils import (
    normalize_date,
    normalize_name,
    normalize_phone_cn,
    normalize_empty,
)

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
BRONZE_TABLE = "co1_bronze"
SILVER_TABLE = "co1_silver"


def main():
    conn_params = {
        "dbname": "your_db",
        "user": "your_user",
        "password": "your_password",
        "host": "your_host",
        "port": "5432",
    }

    with psycopg2.connect(**conn_params) as conn:
        conn.autocommit = True

        # 1) Load bronze data into a Polars DataFrame
        #    Using SQL directly into Polars keeps everything in memory-efficient, columnar format.
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    _dlt_id,
                    _dlt_load_id,
                    contact,
                    target,
                    person_a,
                    person_b,
                    date_of_first_interaction,
                    date_of_last_interaction,
                    additional_info,
                    dataset_name
                FROM {BRONZE_SCHEMA}.{BRONZE_TABLE};
                """
            )
            rows = cur.fetchall()
            colnames = [desc[^0] for desc in cur.description]

        df = pl.DataFrame(rows, schema=colnames)

        # 2) Apply transformations in Polars (vectorized + fast)
        #    All columns remain string-like; we just normalize values.
        df = df.with_columns(
            [
                # phone numbers
                pl.col("contact").map_elements(normalize_phone_cn),
                pl.col("target").map_elements(normalize_phone_cn),
                # names
                pl.col("person_a").map_elements(normalize_name),
                pl.col("person_b").map_elements(normalize_name),
                # dates
                pl.col("date_of_first_interaction").map_elements(normalize_date),
                pl.col("date_of_last_interaction").map_elements(normalize_date),
                # additional info / dataset
                pl.col("additional_info").map_elements(normalize_empty),
                pl.col("dataset_name").map_elements(normalize_empty),
            ]
        )

        # 3) Create the silver table and bulk-insert from Polars
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA};")

            cur.execute(f"DROP TABLE IF EXISTS {SILVER_SCHEMA}.{SILVER_TABLE};")
            cur.execute(
                f"""
                CREATE TABLE {SILVER_SCHEMA}.{SILVER_TABLE} (
                    _dlt_id TEXT,
                    _dlt_load_id TEXT,
                    contact TEXT,
                    target TEXT,
                    person_a TEXT,
                    person_b TEXT,
                    date_of_first_interaction TEXT,
                    date_of_last_interaction TEXT,
                    additional_info TEXT,
                    dataset_name TEXT
                );
                """
            )

            # Convert the Polars DataFrame to Python tuples for executemany
            records = df.select(
                [
                    "_dlt_id",
                    "_dlt_load_id",
                    "contact",
                    "target",
                    "person_a",
                    "person_b",
                    "date_of_first_interaction",
                    "date_of_last_interaction",
                    "additional_info",
                    "dataset_name",
                ]
            ).to_tuples()  # list[tuple]

            insert_sql = f"""
                INSERT INTO {SILVER_SCHEMA}.{SILVER_TABLE} (
                    _dlt_id,
                    _dlt_load_id,
                    contact,
                    target,
                    person_a,
                    person_b,
                    date_of_first_interaction,
                    date_of_last_interaction,
                    additional_info,
                    dataset_name
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """

            cur.executemany(insert_sql, records)


if __name__ == "__main__":
    main()
```

Notes:

- `pl.DataFrame(rows, schema=colnames)` creates a columnar Polars frame from the query result; Polars operations on large frames are typically much faster than equivalent pandas code.[^2][^3][^4][^1]
- `map_elements(...)` lets you reuse your existing Python normalization functions while still benefiting from Polars’ internal speed and memory characteristics.[^6]
- `to_tuples()` gives a list of tuples suitable for `executemany`, which is efficient for bulk inserts with psycopg2.[^7]

If you’d like, I can also show a variant where you read directly from CSV with `pl.read_csv`/`pl.scan_csv` and write to Postgres using `COPY` for even faster throughput.
<span style="display:none">[^10][^11][^12][^13][^14][^15][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://blog.jetbrains.com/pycharm/2024/07/polars-vs-pandas/

[^2]: https://stackoverflow.com/questions/75530375/polars-vs-pandas-size-and-speed-difference

[^3]: https://pola.rs/posts/benchmark-energy-performance/

[^4]: https://realpython.com/polars-vs-pandas/

[^5]: https://stackoverflow.com/questions/71106690/polars-specify-dtypes-for-all-columns-at-once-in-read-csv/76563894

[^6]: https://stuffbyyuki.com/read-csv-files-with-polars-in-python/

[^7]: https://www.psycopg.org/docs/

[^8]: https://www.reddit.com/r/Python/comments/1cy9vpt/speed_improvements_in_polars_over_pandas/

[^9]: https://www.reddit.com/r/Python/comments/1jg402b/polars_vs_pandas/

[^10]: https://www.youtube.com/watch?v=iz1VsZkCpgM

[^11]: https://stackoverflow.com/questions/77098480/polars-psycopg2-write-column-of-lists-to-postgresql

[^12]: https://www.linkedin.com/pulse/polars-vs-pandas-benchmarking-performances-beyond-l6svf

[^13]: https://github.com/ultralytics/ultralytics/issues/21872

[^14]: https://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL

[^15]: https://www.shuttle.dev/blog/2025/09/24/pandas-vs-polars

