# silver_c02.py
import psycopg2
from common_utils import normalize_date, normalize_name, normalize_phone_cn, normalize_empty, get_latest_schema_containing_named_table

BRONZE_SCHEMA_PREFIX = "bronze"
SILVER_SCHEMA = "silver"
TABLE_NAME = "c02"

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
    BRONZE_SCHEMA = get_latest_schema_containing_named_table(conn, BRONZE_SCHEMA_PREFIX, TABLE_NAME)

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
        FROM {BRONZE_SCHEMA}.{TABLE_NAME};
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
            INSERT INTO {SILVER_SCHEMA}.{TABLE_NAME} {} (
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
