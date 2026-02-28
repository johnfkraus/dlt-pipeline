# silver_c01.py
import psycopg2
from psycopg2 import sql
from common_utils import normalize_date, normalize_name, normalize_phone_cn, normalize_empty


def get_latest_schema_with_table(conn, schema_prefix, table_name):
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
        return row[0] if row else None



BRONZE_SCHEMA_PREFIX = "bronze"
#BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
BRONZE_TABLE = "c01_bronze"
SILVER_TABLE = "c01_silver"

def main():
    conn = psycopg2.connect(
        dbname="comms",
        user="postgres",
        password="postgres",
        host="localhost",
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
    # BRONZE_SCHEMA = get_newest_bronze_schema_by_oid(conn)

    BRONZE_SCHEMA =  get_latest_schema_with_table(conn, BRONZE_SCHEMA_PREFIX, BRONZE_TABLE)

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
