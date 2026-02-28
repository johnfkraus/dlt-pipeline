<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Show me examples of how to use a config file to populate the database name user name, password, etc.

You can externalize your Postgres credentials in a few different ways; here are three concrete patterns you can mix and match: INI + `configparser`, `.env` + `dotenv`, and dlt’s `secrets.toml`.[^1][^2][^3][^4][^5][^6][^7]

***

## 1. INI file + `configparser` (classic pattern)

**Step 1 – `database.ini`**

```ini
# database.ini
[postgresql]
host=localhost
port=5432
database=my_database
user=my_app_user
password=my_strong_password
```

**Step 2 – `db_config.py` loader**

```python
# db_config.py
from configparser import ConfigParser
from typing import Dict, Any

def load_db_config(
    filename: str = "database.ini",
    section: str = "postgresql",
) -> Dict[str, Any]:
    parser = ConfigParser()
    parser.read(filename)

    if not parser.has_section(section):
        raise Exception(f"Section {section} not found in {filename}")

    params = {}
    for name, value in parser.items(section):
        params[name] = value
    return params
```

**Step 3 – use in psycopg2 code**

```python
# silver_c01.py (snippet)
import psycopg2
from db_config import load_db_config

def main():
    params = load_db_config()  # reads database.ini
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()
    # ... rest of your logic ...
```

This pattern keeps credentials out of source and is very common in Postgres+Python examples.[^2][^3][^8][^1]

***

## 2. `.env` file + `python-dotenv` (environment variables)

**Step 1 – `.env`**

```bash
# .env
PGHOST=localhost
PGPORT=5432
PGDATABASE=my_database
PGUSER=my_app_user
PGPASSWORD=my_strong_password
```

**Step 2 – `db_env_config.py`**

```python
# db_env_config.py
import os
from dotenv import load_dotenv  # pip install python-dotenv

load_dotenv()  # loads .env from project root

def get_pg_conn_params():
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", "my_database"),
        "user": os.getenv("PGUSER", "my_app_user"),
        "password": os.getenv("PGPASSWORD", "my_strong_password"),
    }
```

**Step 3 – use in psycopg2**

```python
# gold_datamart.py (snippet)
import psycopg2
from db_env_config import get_pg_conn_params

def main():
    conn = psycopg2.connect(**get_pg_conn_params())
    conn.autocommit = True
    cur = conn.cursor()
    # ...
```

This is convenient in containerized or cloud environments and aligns nicely with secrets managers.[^9][^10][^11]

***

## 3. dlt’s `secrets.toml` / `config.toml` for bronze pipelines

For the bronze stage that uses dlt, the recommended way is `.dlt/secrets.toml` and optional `.dlt/config.toml`.[^4][^6][^12][^7]

**Step 1 – `.dlt/secrets.toml`**

```toml
# .dlt/secrets.toml
[destination.postgres.credentials]
database = "my_database"
username = "loader_user"
password = "loader_password"
host     = "localhost"
port     = 5432

# Or DSN style:
# destination.postgres.credentials = "postgresql://loader_user:loader_password@localhost:5432/my_database"
```

**Step 2 – `.dlt/config.toml` (optional)**

```toml
# .dlt/config.toml
[core]
destination = "postgres"
```

**Step 3 – bronze pipeline code (no explicit credentials)**

```python
# bronze_c01.py (snippet)
import dlt

@dlt.resource(name="c01_bronze", write_disposition="merge")
def c01_bronze(...):
    ...

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bronze_c01",
        destination="postgres",   # resolved via config + secrets
        dataset_name="bronze",
    )
    info = pipeline.run(c01_bronze())
    print(info)
```

dlt automatically reads `destination.postgres.credentials` from `secrets.toml` and does not require you to pass a DSN in code.[^6][^12][^4]

***

## 4. Using one config file for both dlt and psycopg2 (optional)

If you want a single source of truth, you can keep the DSN in `secrets.toml` and parse it for psycopg2:

**`secrets.toml`**

```toml
[destination.postgres.credentials]
dsn = "postgresql://my_app_user:my_strong_password@localhost:5432/my_database"
```

**`db_from_secrets.py`**

```python
# db_from_secrets.py
import tomllib  # Python 3.11+
import psycopg2

def get_conn_from_secrets(c01: str = ".dlt/secrets.toml"):
    with open(path, "rb") as f:
        cfg = tomllib.load(f)
    dsn = cfg["destination.postgres.credentials"]["dsn"]
    return psycopg2.connect(dsn)
```

Then use `get_conn_from_secrets()` in your silver/gold scripts, while dlt also uses the same `secrets.toml`.[^4][^6]

***

If you tell me which style you prefer (INI, `.env`, or TOML-only), I can rewrite one of your existing silver/gold scripts to use that configuration pattern end‑to‑end.
<span style="display:none">[^13][^14][^15]</span>

<div align="center">⁂</div>

[^1]: https://neon.com/postgresql/postgresql-python/connect

[^2]: https://www.geeksforgeeks.org/postgresql/postgresql-connecting-to-the-database-using-python/

[^3]: https://gist.github.com/ZacharyMcGuire/d81aa85409594a007fdf80e9fa9b329e

[^4]: https://dlthub.com/docs/general-usage/credentials/complex_types

[^5]: https://pymotw.com/3/configparser/index.html

[^6]: https://dlthub.com/docs/general-usage/credentials/setup

[^7]: https://dlthub.com/docs/walkthroughs/add_credentials

[^8]: https://docs.python.org/3/library/configparser.html

[^9]: https://gist.github.com/ivanleoncz/a7d4f2125135ab4649825252e349aee0

[^10]: https://northflank.com/guides/connecting-to-a-postgre-sql-database-using-python

[^11]: https://wiki.postgresql.org/wiki/Psycopg2_Tutorial

[^12]: https://dlthub.com/docs/dlt-ecosystem/destinations/postgres

[^13]: https://stackoverflow.com/questions/48702801/connecting-to-postgres-sql-with-python

[^14]: https://stackoverflow.com/questions/24674399/how-to-parse-postgresql-configuration-file-in-python

[^15]: https://wiki.python.org/moin/ConfigParserExamples

