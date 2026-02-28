<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Please provide python code for the following:

Using Python, the DLT data load tool, and PostgreSQL, I want to create a data pipeline with bronze, silver and gold stages with the goal of creating a data mart in the gold stage.  The raw data is contained in several Excel spreadsheets.  During the bronze stage, I want to load the data from the spreadsheets into Postgres database tables, one table for each spreadsheet, in a bronze schema/dataset.  
One spreadsheet has columns named “contact”, “target”, “person_a”, “person_b”, “date_of_first_interaction”, “date_of_last_interaction”, “additional_info”, and “dataset_name”.
Two other spreadsheets have columns named “selector_a”, “selector_b”, “name_a”, “name_b”, “date_of_interaction”, “additional_info”, “comment”, and “dataset_name”.
The spreadsheets/datasets are named “c01”, “co2” and “c03”.
During the silver stage, I want to clean and normalize the data from the bronze tables and load it into tables in a silver schema.  I need to normalize dates, names, and phone numbers.  The dates should be normalized into ISO 8601 standard date strings without time zones.  The phone numbers should be normalized based on what would be expected for phone numbers in mainland China.  Names should be normalized by having extra beginning and ending white space trimmed away.
When there is no data (null, empty string, white space only, ‘None’, etc) for a field in a record, the field should say ‘Unknown’.
At all stages, all the database columns should be of the text type.
At the gold stage, create temporary gold table that all have the same column names.
The final column names should be 
‘selector_a’; where applicable rename ‘contact’ to selector_a.  These are phone numbers.
‘selector_b’; where applicable rename ‘contact’ to selector_a.  These are phone numbers. 
Change person_a and person_b into name_a and name_b in the temp gold tables.
Change date_of_first_interaction to date_of_interaction.
Create a jsonb column named ‘other_info’ that contains keys and values for what was in the original ‘additional_info’ column plus values, where applicable, for ‘dataset_name’, ‘date_of_last_interaction’, and ‘comment’.
  Concatenate (UNION ALL)  the temporary gold tables into a single data mart.
There should be one python file for each of the datasets in each of the stages, plus possibly an additional python file to create the data mart table that combines everything.

Below is one possible implementation layout using dlt, pandas, and PostgreSQL.[^1][^2][^3]

Assumptions (adjust as needed):

- Excel files: `c01.xlsx`, `co2.xlsx`, `co3.xlsx` in a local `data/` folder.
- PostgreSQL DSN is configured in `.dlt/secrets.toml` (destination postgres).[^2][^1]
- All DB columns are `TEXT` and schemas are `bronze`, `silver`, `gold`.

***

## Shared helpers (utils)

Create `common_utils.py` used by all stages:

```python
# common_utils.py
import re
from datetime import datetime

UNKNOWN = "Unknown"

def normalize_empty(value: str | None) -> str:
    if value is None:
        return UNKNOWN
    v = str(value).strip()
    if v == "" or v.lower() == "none" or v.lower() == "null":
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
        # remove leading country code if present
        digits = digits[2:]
    if len(digits) == 11:
        return "+86" + digits
    # unknown or malformed
    return UNKNOWN
```


***

## Bronze stage – load Excel into Postgres (text columns)

Each bronze script reads Excel and loads to its own bronze table via dlt.[^3][^4][^2]

### bronze_c01.py

```python
# bronze_c01.py
import dlt
import pandas as pd

PIPELINE_NAME = "bronze_c01"
DATASET_NAME = "bronze"
TABLE_NAME = "c01_bronze"

def extract_c01():
    df = pd.read_excel("data/c01.xlsx", dtype=str)
    # Ensure all columns are strings
    df = df.astype(str)
    return df.to_dict(orient="records")

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="postgres",
        dataset_name=DATASET_NAME,
        full_refresh=True,
    )

    data = extract_c01()

    # All columns as text in Postgres
    load_info = pipeline.run(
        data,
        table_name=TABLE_NAME,
        write_disposition="replace",
        loader_file_format="parquet",
    )
    print(load_info)
```


### bronze_co2.py

```python
# bronze_co2.py
import dlt
import pandas as pd

PIPELINE_NAME = "bronze_co2"
DATASET_NAME = "bronze"
TABLE_NAME = "co2_bronze"

def extract_co2():
    df = pd.read_excel("data/co2.xlsx", dtype=str)
    df = df.astype(str)
    return df.to_dict(orient="records")

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="postgres",
        dataset_name=DATASET_NAME,
        full_refresh=True,
    )

    data = extract_co2()
    load_info = pipeline.run(
        data,
        table_name=TABLE_NAME,
        write_disposition="replace",
        loader_file_format="parquet",
    )
    print(load_info)
```


### bronze_co3.py

```python
# bronze_co3.py
import dlt
import pandas as pd

PIPELINE_NAME = "bronze_co3"
DATASET_NAME = "bronze"
TABLE_NAME = "co3_bronze"

def extract_co3():
    df = pd.read_excel("data/co3.xlsx", dtype=str)
    df = df.astype(str)
    return df.to_dict(orient="records")

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="postgres",
        dataset_name=DATASET_NAME,
        full_refresh=True,
    )

    data = extract_co3()
    load_info = pipeline.run(
        data,
        table_name=TABLE_NAME,
        write_disposition="replace",
        loader_file_format="parquet",
    )
    print(load_info)
```


***

## Silver stage – clean / normalize into silver schema

Silver uses SQL against bronze tables; all columns stay as `TEXT`.

You can run with any PostgreSQL driver (e.g., `psycopg2`).[^5][^3]

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

    # ensure silver schema
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA};")

    # drop & recreate silver table as all TEXT
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

    cur.execute(f"SELECT contact, target, person_a, person_b, "
                f"date_of_first_interaction, date_of_last_interaction, "
                f"additional_info, dataset_name "
                f"FROM {BRONZE_SCHEMA}.{BRONZE_TABLE};")

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


### silver_co2.py (structure matches silver_co3.py; both have same bronze layout)

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

## Gold stage – temp gold tables + data mart

Gold temp tables unify column names and add `other_info` as `jsonb`.

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

## Data mart – union all temp gold tables

Final script unions all temp gold tables into a single mart in `gold` schema.

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
            selector_a TEXT,
            selector_b TEXT,
            name_a TEXT,
            name_b TEXT,
            date_of_interaction TEXT,
            other_info JSONB
        );
        """
    )

    union_sql_parts = []
    for t in TEMP_TABLES:
        union_sql_parts.append(
            f"SELECT selector_a, selector_b, name_a, name_b, date_of_interaction, other_info "
            f"FROM {GOLD_SCHEMA}.{t}"
        )

    union_sql = " UNION ALL ".join(union_sql_parts)

    cur.execute(
        f"INSERT INTO {GOLD_SCHEMA}.{DM_TABLE} "
        f"(selector_a, selector_b, name_a, name_b, date_of_interaction, other_info) "
        f"{union_sql};"
    )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
```


***

## Small table of roles

| Stage | c01 file/script | co2 file/script | co3 file/script | Notes |
| :-- | :-- | :-- | :-- | :-- |
| Bronze | `bronze_c01.py` | `bronze_co2.py` | `bronze_co3.py` | Excel → bronze.$$
co*_bronze
$$ (TEXT) [^2][^3] |
| Silver | `silver_c01.py` | `silver_co2.py` | `silver_co3.py` | Normalize dates, names, phones, `Unknown`. |
| Gold temp | `gold_c01.py` | `gold_co2.py` | `gold_co3.py` | Align columns, build `other_info`. |
| Data mart | `gold_datamart.py` | n/a | n/a | UNION ALL temp gold tables. |

You can now orchestrate these scripts with any scheduler to implement a full bronze–silver–gold pipeline into a unified gold data mart.
<span style="display:none">[^10][^11][^12][^13][^14][^15][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://dlthub.com/docs/pipelines/sql_database_postgres/load-data-with-python-from-sql_database_postgres-to-postgres

[^2]: https://dlthub.com/docs/pipelines/filesystem-local/load-data-with-python-from-filesystem-local-to-postgres

[^3]: https://risingwave.com/blog/guide-to-importing-excel-data-into-postgresql/

[^4]: https://github.com/dlt-hub/dlt

[^5]: https://hinot.alwaysdata.net/astuces/77-some-tips-with-pandas-matplotlib-and-openpyxl?showall=1

[^6]: https://www.youtube.com/watch?v=y9ooIJ7qibU

[^7]: https://www.datacamp.com/tutorial/python-dlt

[^8]: https://www.dbi-services.com/blog/using-dlt-to-get-data-from-db2-to-postgresql/

[^9]: https://pypi.org/project/ChineseNumberUtils/

[^10]: https://www.getorchestra.io/guides/dlt-blueprints-moving-data-from-postgres-rds

[^11]: https://livebook.manning.com/wiki/categories/llm/phone+number

[^12]: https://www.youtube.com/watch?v=j27MxoPDYK0

[^13]: https://blog.devgenius.io/load-an-entire-sql-database-with-three-lines-of-code-using-data-load-tool-dlt-051471a7c93c

[^14]: https://stackoverflow.com/questions/50978414/normalize-phone-numbers-using-python

[^15]: https://github.com/marmelroy/PhoneNumberKit/issues/809

