# bronze_co1.py
import dlt
import pandas as pd

PIPELINE_NAME = "bronze_co1"
DATASET_NAME = "bronze"
TABLE_NAME = "co1_bronze"

def extract_c01():
    df = pd.read_excel("data/c01.xlsx", dtype=str)
    # Ensure all columns are strings
    df = df.astype(str)
    return df.to_dict(orient="records")

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="postgres",
        dataset_name=DATASET_NAME,
        # full_refresh=True,
        dev_mode=True
    )

    data = extract_c01()

    # All columns as text in Postgres
    load_info = pipeline.run(
        data,
        table_name=TABLE_NAME,
        write_disposition="replace",
        loader_file_format="parquet",
    )
    print(load_info)
