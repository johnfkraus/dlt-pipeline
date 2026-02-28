# gold_c01.py
import psycopg2
import json

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
SILVER_TABLE = "c01_silver"
GOLD_TEMP_TABLE = "c01_gold_temp"

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
