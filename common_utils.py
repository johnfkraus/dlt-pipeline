# common_utils.py
import re
from datetime import datetime

UNKNOWN = "Unknown"


def get_latest_schema_containing_named_table(conn, schema_prefix, table_name):
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
