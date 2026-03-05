# bronze_co1.py
import dlt
import pandas as pd

PIPELINE_NAME = "bronze_co1"
DATASET_NAME = "bronze"
TABLE_NAME = "co1_bronze"

def extract_co1():
    df = pd.read_excel("data/co1.xlsx", dtype=str)
    df = df.astype(str)
    return df.to_dict(orient="records")

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="postgres",
        dataset_name=DATASET_NAME,
        dev_mode=True
    )

    data = extract_co1()

    # Apply schema hint: add bronze_row_uuid as UUID PK with gen_random_uuid() default
    # dlt will generate the column and use Postgres' uuid-ossp extension automatically
    pipeline.run(
        data,
        table_name=TABLE_NAME,
        write_disposition="replace",
        loader_file_format="parquet",
        # Schema hints: add the UUID PK column
        columns={
            "_dlt_id": {"data_type": "text"},  # keep dlt system columns
            "_dlt_load_id": {"data_type": "text"},
            "bronze_row_uuid": {
                "data_type": "uuid",
                "primary_key": True,
                "nullable": False,
                # Postgres default: gen_random_uuid() (requires uuid-ossp extension)
                "default": "gen_random_uuid()",
            },
            # all other columns inferred from data will be text
        },
    )
    print("Load info:", load_info)
