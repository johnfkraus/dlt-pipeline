<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# For all the original python code examples, show me complete code that uses dlt decorators with column type hints like this:

@dlt.resource(
name="c01_bronze",
write_disposition="replace",
columns={
...etc.

Below is a complete rewrite of the earlier examples so that:

- Bronze uses `@dlt.resource(..., columns={...})` with `data_type="text"` everywhere.[^1][^2]
- Silver and gold still use psycopg2, but you *could* also wrap them as dlt resources if you wanted.

I keep the structure you asked for: one Python file per dataset per stage, plus a data mart script.

***

## common_utils.py (unchanged logic)

```python
# common_utils.py
import re
from datetime import datetime

UNKNOWN = "Unknown"

def normalize_empty(value: str | None) -> str:
    if value is None:
        return UNKNOWN
    v = str(value).strip()
    if v == "" or v.lower() in ("none", "null"):
        return UNKNOWN
    return v

def normalize_date(value: str | None) -> str:
    v = normalize_empty(value)
    if v == UNKNOWN:
        return v
    candidates = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%Y%m%d",
    ]
    for fmt in candidates:
        try:
            dt = datetime.strptime(v, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return UNKNOWN

def normalize_name(value: str | None) -> str:
    v = normalize_empty(value)
    if v == UNKNOWN:
        return v
    return v.strip()

def normalize_phone_cn(value: str | None) -> str:
    v = normalize_empty(value)
    if v == UNKNOWN:
        return v
    digits = re.sub(r"\D", "", v)
    if digits.startswith("86") and len(digits) > 11:
        digits = digits[2:]
    if len(digits) == 11:
        return "+86" + digits
    return UNKNOWN
```


***

## Bronze stage – now using `@dlt.resource(..., columns={...})`

dlt infers types from data, but the `columns` hint lets you pin everything to `text` in the destination.[^2][^1]

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
    write_disposition="replace",
    columns={
        "contact": {"data_type": "text"},
        "target": {"data_type": "text"},
        "person_a": {"data_type": "text"},
        "person_b": {"data_type": "text"},
        "date_of_first_interaction": {"data_type": "text"},
        "date_of_last_interaction": {"data_type": "text"},
        "additional_info": {"data_type": "text"},
        "dataset_name": {"data_type": "text"},
    },
)
def c01_bronze(c01: str = "data/c01.csv") -> Iterator[Co1Row]:
    df = pd.read_csv(c01, dtype=str).fillna("")
    for rec in df.to_dict(orient="records"):
        yield Co1Row(**rec)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bronze_c01",
        destination="postgres",
        dataset_name="bronze",
        full_refresh=True,
    )
    load_info = pipeline.run(c01_bronze())
    print(load_info)
```


***

### bronze_c02.py

```python
# bronze_c02.py
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
    name="c02_bronze",
    write_disposition="replace",
    columns={
        "selector_a": {"data_type": "text"},
        "selector_b": {"data_type": "text"},
        "name_a": {"data_type": "text"},
        "name_b": {"data_type": "text"},
        "date_of_interaction": {"data_type": "text"},
        "additional_info": {"data_type": "text"},
        "comment": {"data_type": "text"},
        "dataset_name": {"data_type": "text"},
    },
)
def c02_bronze(c01: str = "data/c02.csv") -> Iterator[Co2Row]:
    df = pd.read_csv(c01, dtype=str).fillna("")
    for rec in df.to_dict(orient="records"):
        yield Co2Row(**rec)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bronze_c02",
        destination="postgres",
        dataset_name="bronze",
        full_refresh=True,
    )
    load_info = pipeline.run(c02_bronze())
    print(load_info)
```


***

### bronze_c03.py

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
    write_disposition="replace",
    columns={
        "selector_a": {"data_type": "text"},
        "selector_b": {"data_type": "text"},
        "name_a": {"data_type": "text"},
        "name_b": {"data_type": "text"},
        "date_of_interaction": {"data_type": "text"},
        "additional_info": {"data_type": "text"},
        "comment": {"data_type": "text"},
        "dataset_name": {"data_type": "text"},
    },
)
def co3_bronze(c01: str = "data/co3.csv") -> Iterator[Co3Row]:
    df = pd.read_csv(c01, dtype=str).fillna("")
    for rec in df.to_dict(orient="records"):
        yield Co3Row(**rec)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bronze_co3",
        destination="postgres",
        dataset_name="bronze",
        full_refresh=True,
    )
    load_info = pipeline.run(co3_bronze())
    print(load_info)
```


***

## Silver stage – psycopg2 (no change needed for dlt, but complete code)

You can keep silver as plain SQL, since the contract “all columns text” is already enforced in bronze by dlt hints.[^1]

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
        user="postgres",
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
        SELECT contact, target, person_a, person_b,
               date_of_first_interaction, date_of_last_interaction,
               additional_info, dataset_name
        FROM {BRONZE_SCHEMA}.{BRONZE_TABLE};
        """
    )

    rows = cur.fetchall()
    for row in rows:
        contact, target, person_a, person_b, d_first, d_last, add_info, dataset = row

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
                contact, target, person_a, person_b,
                date_of_first_interaction, date_of_last_interaction,
                additional_info, dataset_name
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
            """,
            (
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
        user="postgres",
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
        SELECT selector_a, selector_b, name_a, name_b,
               date_of_interaction, additional_info, comment, dataset_name
        FROM {BRONZE_SCHEMA}.{BRONZE_TABLE};
        """
    )

    rows = cur.fetchall()
    for row in rows:
        selector_a, selector_b, name_a, name_b, d_int, add_info, comment, dataset = row

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
                selector_a, selector_b, name_a, name_b,
                date_of_interaction, additional_info, comment, dataset_name
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
            """,
            (
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
        user="postgres",
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
        SELECT selector_a, selector_b, name_a, name_b,
               date_of_interaction, additional_info, comment, dataset_name
        FROM {BRONZE_SCHEMA}.{BRONZE_TABLE};
        """
    )

    rows = cur.fetchall()
    for row in rows:
        selector_a, selector_b, name_a, name_b, d_int, add_info, comment, dataset = row

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
                selector_a, selector_b, name_a, name_b,
                date_of_interaction, additional_info, comment, dataset_name
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
            """,
            (
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

## Gold stage – temporary gold tables and data mart

Gold still uses plain SQL, but everything is text except `other_info` (jsonb) as you requested.

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
        user="postgres",
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
        SELECT contact, target, person_a, person_b,
               date_of_first_interaction, date_of_last_interaction,
               additional_info, dataset_name
        FROM {SILVER_SCHEMA}.{SILVER_TABLE};
        """
    )

    rows = cur.fetchall()
    for row in rows:
        contact, target, person_a, person_b, d_first, d_last, additional, dataset = row

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
                selector_a, selector_b, name_a, name_b,
                date_of_interaction, other_info
            )
            VALUES (%s,%s,%s,%s,%s,%s::jsonb);
            """,
            (
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
        user="postgres",
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
        SELECT selector_a, selector_b, name_a, name_b,
               date_of_interaction, additional_info, comment, dataset_name
        FROM {SILVER_SCHEMA}.{SILVER_TABLE};
        """
    )

    rows = cur.fetchall()
    for row in rows:
        selector_a, selector_b, name_a, name_b, d_int, additional, comment, dataset = row

        other = {
            "additional_info": additional,
            "dataset_name": dataset,
            "comment": comment,
        }

        cur.execute(
            f"""
            INSERT INTO {GOLD_SCHEMA}.{GOLD_TEMP_TABLE} (
                selector_a, selector_b, name_a, name_b,
                date_of_interaction, other_info
            )
            VALUES (%s,%s,%s,%s,%s,%s::jsonb);
            """,
            (
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
        user="postgres",
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
        SELECT selector_a, selector_b, name_a, name_b,
               date_of_interaction, additional_info, comment, dataset_name
        FROM {SILVER_SCHEMA}.{SILVER_TABLE};
        """
    )

    rows = cur.fetchall()
    for row in rows:
        selector_a, selector_b, name_a, name_b, d_int, additional, comment, dataset = row

        other = {
            "additional_info": additional,
            "dataset_name": dataset,
            "comment": comment,
        }

        cur.execute(
            f"""
            INSERT INTO {GOLD_SCHEMA}.{GOLD_TEMP_TABLE} (
                selector_a, selector_b, name_a, name_b,
                date_of_interaction, other_info
            )
            VALUES (%s,%s,%s,%s,%s,%s::jsonb);
            """,
            (
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

### gold_datamart.py

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
        user="postgres",
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
        f"SELECT selector_a, selector_b, name_a, name_b, date_of_interaction, other_info "
        f"FROM {GOLD_SCHEMA}.{t}"
        for t in TEMP_TABLES
    ]
    union_sql = " UNION ALL ".join(union_sql_parts)

    cur.execute(
        f"""
        INSERT INTO {GOLD_SCHEMA}.{DM_TABLE} (
            selector_a, selector_b, name_a, name_b, date_of_interaction, other_info
        )
        {union_sql};
        """
    )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
```

This gives you dlt-style column hints on all bronze resources, guaranteeing `text` for every column at the destination, while preserving the rest of your bronze–silver–gold pipeline structure.[^3][^2][^1]
<span style="display:none">[^10][^11][^12][^13][^14][^15][^4][^5][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://dlthub.com/docs/general-usage/schema

[^2]: https://raw.githubusercontent.com/dlt-hub/dlt/refs/heads/devel/docs/website/docs/general-usage/resource.md

[^3]: https://dev.to/pizofreude/study-notes-dlt-fundamentals-course-lesson-5-6-write-disposition-incremental-loading-how-dlt-429j

[^4]: https://dlthub.com/docs/general-usage/resource

[^5]: https://dlthub.com/docs/general-usage/schema-contracts

[^6]: https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_7_data_contracts.ipynb

[^7]: https://docs.getdbt.com/reference/resource-properties/columns

[^8]: https://github.com/dlt-hub/dlt/issues/2659

[^9]: https://github.com/dlt-hub/dlt/issues/2788

[^10]: https://github.com/dlt-hub/dlt/issues/242

[^11]: https://dlthub.com/docs/reference/frequently-asked-questions

[^12]: https://aetperf.github.io/data engineering/python/2025/11/27/An-Example-ETL-Pipeline-with-dlt-SQLMesh-DuckDB.html

[^13]: https://stackoverflow.com/questions/79633200/dlt-fails-to-load-table-if-destination-database-already-contains-tables

[^14]: https://github.com/dlt-hub/verified-sources/blob/master/docs/BUILDING-BLOCKS.md

[^15]: https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_6_write_disposition_strategies_and_advanced_tricks.ipynb

