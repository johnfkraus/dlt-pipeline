# gold_datamart.py
import psycopg2

GOLD_SCHEMA = "gold"
DM_TABLE = "interactions_mart"

TEMP_TABLES = [
    "c01_gold_temp",
    "localhost_gold_temp",
    "c03_gold_temp",
]

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

    cur.execute(f"DROP TABLE IF EXISTS {GOLD_SCHEMA}.{DM_TABLE};")
    cur.execute(
        f"""
        CREATE TABLE {GOLD_SCHEMA}.{DM_TABLE} (
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

    union_sql_parts = [
        f"""
        SELECT
            _dlt_id,
            _dlt_load_id,
            selector_a,
            selector_b,
            name_a,
            name_b,
            date_of_interaction,
            other_info
        FROM {GOLD_SCHEMA}.{t}
        """
        for t in TEMP_TABLES
    ]
    union_sql = " UNION ALL ".join(union_sql_parts)

    cur.execute(
        f"""
        INSERT INTO {GOLD_SCHEMA}.{DM_TABLE} (
            _dlt_id,
            _dlt_load_id,
            selector_a,
            selector_b,
            name_a,
            name_b,
            date_of_interaction,
            other_info
        )
        {union_sql};
        """
    )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
