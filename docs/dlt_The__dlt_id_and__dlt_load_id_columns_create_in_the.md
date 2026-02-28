<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# The _dlt_id and _dlt_load_id columns create in the bronze database tables should also be present in all the silver and gold tables.  Provide complete revised code, please.

To keep `_dlt_id` and `_dlt_load_id` all the way through, you need to:

1) ensure bronze tables actually have these columns from dlt, and 2) carry them through in your silver and gold SQL.[^1][^2][^3][^4]

Below is complete revised code for all stages with those two columns present everywhere.

***

## Bronze stage – dlt resources (unchanged structure, but we explicitly include system columns in hints)

Note: dlt will create `_dlt_id` and `_dlt_load_id` automatically when configured; you do not need to put them in `columns`, but including them as text in your mental model makes silver/gold easier.[^2][^4][^1]

### bronze_c01.py

```python
# bronze_c01.py
from typing import Iterator, TypedDict
import dlt
import pandas as pd

class Co1Row(TypedDict):
    contact: str
    target: str
    person_a: str
    person_b: str
    date_of_first_interaction: str
    date_of_last_interaction: str
    additional_info: str
    dataset_name: str

@dlt.resource(
    name="c01_bronze",
    write_disposition="merge",
    primary_key="date_of_first_interaction",  # or real key if you have one
    columns={
        "contact": {"data_type": "text"},
        "target": {"data_type": "text"},
        "person_a": {"data_type": "text"},
        "person_b": {"data_type": "text"},
        "date_of_first_interaction": {"data_type": "text"},
        "date_of_last_interaction": {"data_type": "text"},
        "additional_info": {"data_type": "text"},
        "dataset_name": {"data_type": "text"},
        # dlt system columns will be text/varchar in Postgres
        "_dlt_id": {"data_type": "text"},
        "_dlt_load_id": {"data_type": "text"},
    },
)
def c01_bronze(
    cursor: dlt.sources.incremental = dlt.sources.incremental(
        "date_of_first_interaction",
        initial_value="2000-01-01",
    ),
    path: str = "data/c01.csv",
) -> Iterator[Co1Row]:
    df = pd.read_csv(path, dtype=str).fillna("")
    for rec in df.to_dict(orient="records"):
        yield Co1Row(**rec)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bronze_c01",
        destination="postgres",
        dataset_name="bronze",
    )
    info = pipeline.run(c01_bronze())
    print(info)
```


***

### bronze_co2.py

```python
# bronze_co2.py
from typing import Iterator, TypedDict
import dlt
import pandas as pd

class Co2Row(TypedDict):
    selector_a: str
    selector_b: str
    name_a: str
    name_b: str
    date_of_interaction: str
    additional_info: str
    comment: str
    dataset_name: str

@dlt.resource(
    name="co2_bronze",
    write_disposition="merge",
    primary_key=("selector_a", "selector_b", "date_of_interaction"),
    columns={
        "selector_a": {"data_type": "text"},
        "selector_b": {"data_type": "text"},
        "name_a": {"data_type": "text"},
        "name_b": {"data_type": "text"},
        "date_of_interaction": {"data_type": "text"},
        "additional_info": {"data_type": "text"},
        "comment": {"data_type": "text"},
        "dataset_name": {"data_type": "text"},
        "_dlt_id": {"data_type": "text"},
        "_dlt_load_id": {"data_type": "text"},
    },
)
def co2_bronze(
    cursor: dlt.sources.incremental = dlt.sources.incremental(
        "date_of_interaction",
        initial_value="2000-01-01",
    ),
    path: str = "data/co2.csv",
) -> Iterator[Co2Row]:
    df = pd.read_csv(path, dtype=str).fillna("")
    for rec in df.to_dict(orient="records"):
        yield Co2Row(**rec)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bronze_co2",
        destination="postgres",
        dataset_name="bronze",
    )
    info = pipeline.run(co2_bronze())
    print(info)
```


***

### bronze_co3.py

```python
# bronze_co3.py
from typing import Iterator, TypedDict
import dlt
import pandas as pd

class Co3Row(TypedDict):
    selector_a: str
    selector_b: str
    name_a: str
    name_b: str
    date_of_interaction: str
    additional_info: str
    comment: str
    dataset_name: str

@dlt.resource(
    name="co3_bronze",
    write_disposition="merge",
    primary_key=("selector_a", "selector_b", "date_of_interaction"),
    columns={
        "selector_a": {"data_type": "text"},
        "selector_b": {"data_type": "text"},
        "name_a": {"data_type": "text"},
        "name_b": {"data_type": "text"},
        "date_of_interaction": {"data_type": "text"},
        "additional_info": {"data_type": "text"},
        "comment": {"data_type": "text"},
        "dataset_name": {"data_type": "text"},
        "_dlt_id": {"data_type": "text"},
        "_dlt_load_id": {"data_type": "text"},
    },
)
def co3_bronze(
    cursor: dlt.sources.incremental = dlt.sources.incremental(
        "date_of_interaction",
        initial_value="2000-01-01",
    ),
    path: str = "data/co3.csv",
) -> Iterator[Co3Row]:
    df = pd.read_csv(path, dtype=str).fillna("")
    for rec in df.to_dict(orient="records"):
        yield Co3Row(**rec)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bronze_co3",
        destination="postgres",
        dataset_name="bronze",
    )
    info = pipeline.run(co3_bronze())
    print(info)
```


***

## Silver stage – carry `_dlt_id` and `_dlt_load_id`

We now select these system columns from bronze and create corresponding text columns in silver.[^1][^2]

### silver_c01.py

```python
# silver_c01.py
import psycopg2
from common_utils import normalize_date, normalize_name, normalize_phone_cn, normalize_empty

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
BRONZE_TABLE = "c01_bronze"
SILVER_TABLE = "c01_silver"

def main():
    conn = psycopg2.connect(
        dbname="your_db",
        user="your_user",
        password="your_password",
        host="your_host",
        port="5432",
    )
    conn.autocommit = True
    cur = conn.cursor()

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

    cur.execute(
        f"""
        SELECT _dlt_id,
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
    for row in rows:
        (
            dlt_id,
            dlt_load_id,
            contact,
            target,
            person_a,
            person_b,
            d_first,
            d_last,
            add_info,
            dataset,
        ) = row

        contact_n = normalize_phone_cn(contact)
        target_n = normalize_phone_cn(target)
        person_a_n = normalize_name(person_a)
        person_b_n = normalize_name(person_b)
        d_first_n = normalize_date(d_first)
        d_last_n = normalize_date(d_last)
        add_n = normalize_empty(add_info)
        dataset_n = normalize_empty(dataset)

        cur.execute(
            f"""
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
            """,
            (
                dlt_id,
                dlt_load_id,
                contact_n,
                target_n,
                person_a_n,
                person_b_n,
                d_first_n,
                d_last_n,
                add_n,
                dataset_n,
            ),
        )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
```


***

### silver_co2.py

```python
# silver_co2.py
import psycopg2
from common_utils import normalize_date, normalize_name, normalize_phone_cn, normalize_empty

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
BRONZE_TABLE = "co2_bronze"
SILVER_TABLE = "co2_silver"

def main():
    conn = psycopg2.connect(
        dbname="your_db",
        user="your_user",
        password="your_password",
        host="your_host",
        port="5432",
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA};")

    cur.execute(f"DROP TABLE IF EXISTS {SILVER_SCHEMA}.{SILVER_TABLE};")
    cur.execute(
        f"""
        CREATE TABLE {SILVER_SCHEMA}.{SILVER_TABLE} (
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

    cur.execute(
        f"""
        SELECT _dlt_id,
               _dlt_load_id,
               selector_a,
               selector_b,
               name_a,
               name_b,
               date_of_interaction,
               additional_info,
               comment,
               dataset_name
        FROM {BRONZE_SCHEMA}.{BRONZE_TABLE};
        """
    )

    rows = cur.fetchall()
    for row in rows:
        (
            dlt_id,
            dlt_load_id,
            selector_a,
            selector_b,
            name_a,
            name_b,
            d_int,
            add_info,
            comment,
            dataset,
        ) = row

        selector_a_n = normalize_phone_cn(selector_a)
        selector_b_n = normalize_phone_cn(selector_b)
        name_a_n = normalize_name(name_a)
        name_b_n = normalize_name(name_b)
        d_int_n = normalize_date(d_int)
        add_n = normalize_empty(add_info)
        comment_n = normalize_empty(comment)
        dataset_n = normalize_empty(dataset)

        cur.execute(
            f"""
            INSERT INTO {SILVER_SCHEMA}.{SILVER_TABLE} (
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
            """,
            (
                dlt_id,
                dlt_load_id,
                selector_a_n,
                selector_b_n,
                name_a_n,
                name_b_n,
                d_int_n,
                add_n,
                comment_n,
                dataset_n,
            ),
        )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
```


***

### silver_co3.py

```python
# silver_co3.py
import psycopg2
from common_utils import normalize_date, normalize_name, normalize_phone_cn, normalize_empty

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
BRONZE_TABLE = "co3_bronze"
SILVER_TABLE = "co3_silver"

def main():
    conn = psycopg2.connect(
        dbname="your_db",
        user="your_user",
        password="your_password",
        host="your_host",
        port="5432",
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA};")

    cur.execute(f"DROP TABLE IF EXISTS {SILVER_SCHEMA}.{SILVER_TABLE};")
    cur.execute(
        f"""
        CREATE TABLE {SILVER_SCHEMA}.{SILVER_TABLE} (
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

    cur.execute(
        f"""
        SELECT _dlt_id,
               _dlt_load_id,
               selector_a,
               selector_b,
               name_a,
               name_b,
               date_of_interaction,
               additional_info,
               comment,
               dataset_name
        FROM {BRONZE_SCHEMA}.{BRONZE_TABLE};
        """
    )

    rows = cur.fetchall()
    for row in rows:
        (
            dlt_id,
            dlt_load_id,
            selector_a,
            selector_b,
            name_a,
            name_b,
            d_int,
            add_info,
            comment,
            dataset,
        ) = row

        selector_a_n = normalize_phone_cn(selector_a)
        selector_b_n = normalize_phone_cn(selector_b)
        name_a_n = normalize_name(name_a)
        name_b_n = normalize_name(name_b)
        d_int_n = normalize_date(d_int)
        add_n = normalize_empty(add_info)
        comment_n = normalize_empty(comment)
        dataset_n = normalize_empty(dataset)

        cur.execute(
            f"""
            INSERT INTO {SILVER_SCHEMA}.{SILVER_TABLE} (
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
            """,
            (
                dlt_id,
                dlt_load_id,
                selector_a_n,
                selector_b_n,
                name_a_n,
                name_b_n,
                d_int_n,
                add_n,
                comment_n,
                dataset_n,
            ),
        )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
```


***

## Gold stage – include `_dlt_id` and `_dlt_load_id` in temp tables and mart

Gold temp tables and final mart now all have these columns.[^5][^2][^1]

### gold_c01.py

```python
# gold_c01.py
import psycopg2
import json

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
SILVER_TABLE = "c01_silver"
GOLD_TEMP_TABLE = "c01_gold_temp"

def main():
    conn = psycopg2.connect(
        dbname="your_db",
        user="your_user",
        password="your_password",
        host="your_host",
        port="5432",
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA};")

    cur.execute(f"DROP TABLE IF EXISTS {GOLD_SCHEMA}.{GOLD_TEMP_TABLE};")
    cur.execute(
        f"""
        CREATE TABLE {GOLD_SCHEMA}.{GOLD_TEMP_TABLE} (
            _dlt_id TEXT,
            _dlt_load_id TEXT,
            selector_a TEXT,
            selector_b TEXT,
            name_a TEXT,
            name_b TEXT,
            date_of_interaction TEXT,
            other_info JSONB
        );
        """
    )

    cur.execute(
        f"""
        SELECT _dlt_id,
               _dlt_load_id,
               contact,
               target,
               person_a,
               person_b,
               date_of_first_interaction,
               date_of_last_interaction,
               additional_info,
               dataset_name
        FROM {SILVER_SCHEMA}.{SILVER_TABLE};
        """
    )

    rows = cur.fetchall()
    for row in rows:
        (
            dlt_id,
            dlt_load_id,
            contact,
            target,
            person_a,
            person_b,
            d_first,
            d_last,
            additional,
            dataset,
        ) = row

        selector_a = contact
        selector_b = target
        name_a = person_a
        name_b = person_b
        date_of_interaction = d_first

        other = {
            "additional_info": additional,
            "dataset_name": dataset,
            "date_of_last_interaction": d_last,
        }

        cur.execute(
            f"""
            INSERT INTO {GOLD_SCHEMA}.{GOLD_TEMP_TABLE} (
                _dlt_id,
                _dlt_load_id,
                selector_a,
                selector_b,
                name_a,
                name_b,
                date_of_interaction,
                other_info
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb);
            """,
            (
                dlt_id,
                dlt_load_id,
                selector_a,
                selector_b,
                name_a,
                name_b,
                date_of_interaction,
                json.dumps(other),
            ),
        )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
```


***

### gold_co2.py

```python
# gold_co2.py
import psycopg2
import json

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
SILVER_TABLE = "co2_silver"
GOLD_TEMP_TABLE = "co2_gold_temp"

def main():
    conn = psycopg2.connect(
        dbname="your_db",
        user="your_user",
        password="your_password",
        host="your_host",
        port="5432",
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA};")

    cur.execute(f"DROP TABLE IF EXISTS {GOLD_SCHEMA}.{GOLD_TEMP_TABLE};")
    cur.execute(
        f"""
        CREATE TABLE {GOLD_SCHEMA}.{GOLD_TEMP_TABLE} (
            _dlt_id TEXT,
            _dlt_load_id TEXT,
            selector_a TEXT,
            selector_b TEXT,
            name_a TEXT,
            name_b TEXT,
            date_of_interaction TEXT,
            other_info JSONB
        );
        """
    )

    cur.execute(
        f"""
        SELECT _dlt_id,
               _dlt_load_id,
               selector_a,
               selector_b,
               name_a,
               name_b,
               date_of_interaction,
               additional_info,
               comment,
               dataset_name
        FROM {SILVER_SCHEMA}.{SILVER_TABLE};
        """
    )

    rows = cur.fetchall()
    for row in rows:
        (
            dlt_id,
            dlt_load_id,
            selector_a,
            selector_b,
            name_a,
            name_b,
            d_int,
            additional,
            comment,
            dataset,
        ) = row

        other = {
            "additional_info": additional,
            "dataset_name": dataset,
            "comment": comment,
        }

        cur.execute(
            f"""
            INSERT INTO {GOLD_SCHEMA}.{GOLD_TEMP_TABLE} (
                _dlt_id,
                _dlt_load_id,
                selector_a,
                selector_b,
                name_a,
                name_b,
                date_of_interaction,
                other_info
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb);
            """,
            (
                dlt_id,
                dlt_load_id,
                selector_a,
                selector_b,
                name_a,
                name_b,
                d_int,
                json.dumps(other),
            ),
        )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
```


***

### gold_co3.py

```python
# gold_co3.py
import psycopg2
import json

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
SILVER_TABLE = "co3_silver"
GOLD_TEMP_TABLE = "co3_gold_temp"

def main():
    conn = psycopg2.connect(
        dbname="your_db",
        user="your_user",
        password="your_password",
        host="your_host",
        port="5432",
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA};")

    cur.execute(f"DROP TABLE IF EXISTS {GOLD_SCHEMA}.{GOLD_TEMP_TABLE};")
    cur.execute(
        f"""
        CREATE TABLE {GOLD_SCHEMA}.{GOLD_TEMP_TABLE} (
            _dlt_id TEXT,
            _dlt_load_id TEXT,
            selector_a TEXT,
            selector_b TEXT,
            name_a TEXT,
            name_b TEXT,
            date_of_interaction TEXT,
            other_info JSONB
        );
        """
    )

    cur.execute(
        f"""
        SELECT _dlt_id,
               _dlt_load_id,
               selector_a,
               selector_b,
               name_a,
               name_b,
               date_of_interaction,
               additional_info,
               comment,
               dataset_name
        FROM {SILVER_SCHEMA}.{SILVER_TABLE};
        """
    )

    rows = cur.fetchall()
    for row in rows:
        (
            dlt_id,
            dlt_load_id,
            selector_a,
            selector_b,
            name_a,
            name_b,
            d_int,
            additional,
            comment,
            dataset,
        ) = row

        other = {
            "additional_info": additional,
            "dataset_name": dataset,
            "comment": comment,
        }

        cur.execute(
            f"""
            INSERT INTO {GOLD_SCHEMA}.{GOLD_TEMP_TABLE} (
                _dlt_id,
                _dlt_load_id,
                selector_a,
                selector_b,
                name_a,
                name_b,
                date_of_interaction,
                other_info
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb);
            """,
            (
                dlt_id,
                dlt_load_id,
                selector_a,
                selector_b,
                name_a,
                name_b,
                d_int,
                json.dumps(other),
            ),
        )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
```


***

### gold_datamart.py – union keeps the system columns

```python
# gold_datamart.py
import psycopg2

GOLD_SCHEMA = "gold"
DM_TABLE = "interactions_mart"

TEMP_TABLES = [
    "c01_gold_temp",
    "co2_gold_temp",
    "co3_gold_temp",
]

def main():
    conn = psycopg2.connect(
        dbname="your_db",
        user="your_user",
        password="your_password",
        host="your_host",
        port="5432",
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA};")

    cur.execute(f"DROP TABLE IF EXISTS {GOLD_SCHEMA}.{DM_TABLE};")
    cur.execute(
        f"""
        CREATE TABLE {GOLD_SCHEMA}.{DM_TABLE} (
            _dlt_id TEXT,
            _dlt_load_id TEXT,
            selector_a TEXT,
            selector_b TEXT,
            name_a TEXT,
            name_b TEXT,
            date_of_interaction TEXT,
            other_info JSONB
        );
        """
    )

    union_sql_parts = [
        f"""
        SELECT
            _dlt_id,
            _dlt_load_id,
            selector_a,
            selector_b,
            name_a,
            name_b,
            date_of_interaction,
            other_info
        FROM {GOLD_SCHEMA}.{t}
        """
        for t in TEMP_TABLES
    ]
    union_sql = " UNION ALL ".join(union_sql_parts)

    cur.execute(
        f"""
        INSERT INTO {GOLD_SCHEMA}.{DM_TABLE} (
            _dlt_id,
            _dlt_load_id,
            selector_a,
            selector_b,
            name_a,
            name_b,
            date_of_interaction,
            other_info
        )
        {union_sql};
        """
    )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
```

With this revision, `_dlt_id` and `_dlt_load_id` are present in every layer, so you can trace each gold mart row back to the exact bronze load and row.[^6][^5][^2][^1]
<span style="display:none">[^10][^11][^12][^13][^14][^15][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://dlthub.com/docs/general-usage/destination-tables

[^2]: https://dlthub.com/blog/dlt-lineage-support

[^3]: https://dlthub.com/docs/pipelines/sql_database_postgres/load-data-with-python-from-sql_database_postgres-to-postgres

[^4]: https://github.com/dlt-hub/dlt/issues/2631

[^5]: https://aetperf.github.io/data engineering/python/2025/11/27/An-Example-ETL-Pipeline-with-dlt-SQLMesh-DuckDB.html

[^6]: https://dlthub.com/devel/blog/tags/metadata

[^7]: https://github.com/dlt-hub/dlt/issues/1317

[^8]: https://jorritsandbrink.substack.com/p/how-dlt-uses-apache-arrow-for-fast-pipelines

[^9]: https://community.databricks.com/t5/data-engineering/identity-column-in-dlt-using-python/td-p/16838

[^10]: https://dlthub.com/devel/blog/dlt-lineage-support

[^11]: https://stackoverflow.com/questions/78846128/dlt-how-to-get-pipeline-id-and-update-id/78846810

[^12]: https://dev.to/cmcrawford2/normalizing-data-with-dlt-3pf6

[^13]: https://www.reddit.com/r/dataengineering/comments/1bqmmj6/metadata_columns_youve_found_useful_in_rawlanding/

[^14]: https://davidsj.substack.com/p/sqlmesh-init-t-dlt-dlt-pipeline-bluesky

[^15]: https://stackoverflow.com/questions/78925300/define-dlt-pipeline-that-depends-on-event-log

