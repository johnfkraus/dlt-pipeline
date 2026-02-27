# silver_co1.py
import psycopg2
from common_utils import normalize_date, normalize_name, normalize_phone_cn, normalize_empty

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
BRONZE_TABLE = "co1_bronze"
SILVER_TABLE = "co1_silver"

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
