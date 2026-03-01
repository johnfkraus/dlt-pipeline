# common_utils.py
import re
import tomllib
from pathlib import Path
from datetime import datetime

UNKNOWN = "Unknown"   # or 'None' or 'N/A'?
NONE = "None"


def get_latest_schema_containing_named_table(conn, schema_prefix, table_name) -> str:
    """
    Return the name of the most recently created schema (by OID)
    whose name starts with `schema_prefix` and that contains
    a base table named `table_name`. Returns None if not found.
    """
    query = """
        SELECT n.nspname AS schema_name
        FROM pg_catalog.pg_namespace n
        JOIN information_schema.tables t
          ON t.table_schema = n.nspname
        WHERE n.nspname LIKE %(prefix)s
          AND t.table_name = %(table)s
          AND t.table_type = 'BASE TABLE'
        ORDER BY n.oid DESC
        LIMIT 1;
    """

    with conn.cursor() as cur:
        cur.execute(
            query,
            {
                "prefix": schema_prefix + "%",  # prefix match
                "table": table_name
            }
        )
        row = cur.fetchone()
        result = row[0] if row else None
        print(f"get_latest_schema_containing_named_table: {result}")
        # return row[0] if row else None

        return result


def normalize_empty(value: str | None) -> str:
    if value is None or value == "" or value == NONE:
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
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%Y%m%d",
        "%Y %m %d",
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
    if value is None or value == NONE:
        return NONE
    v = normalize_empty(value)
    if v == UNKNOWN:
        return v
    digits = re.sub(r"\D", "", v)
    if digits.startswith("86") and len(digits) > 11:
        digits = digits[2:]
    if len(digits) == 11:
        return "+86" + digits
    return UNKNOWN


def normalize_phone_cn_target(value: str | None) -> str:
    if value is None or value == NONE:
        return NONE
    v = normalize_empty(value)
    if v == UNKNOWN:
        return v
    digits = re.sub(r"\D", "", v)
    if digits.startswith("86") and len(digits) > 11:
        digits = digits[2:]
    if len(digits) == 11:
        return "+86" + digits
    return UNKNOWN


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
