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

