# bronze_co3.py
import dlt
import pandas as pd

PIPELINE_NAME = "bronze_co3"
DATASET_NAME = "bronze"
TABLE_NAME = "co3_bronze"

def extract_co3():
    df = pd.read_excel("data/co3.xlsx", dtype=str)
    df = df.astype(str)
    return df.to_dict(orient="records")

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="postgres",
        dataset_name=DATASET_NAME,
        full_refresh=True,
    )

    data = extract_co3()
    load_info = pipeline.run(
        data,
        table_name=TABLE_NAME,
        write_disposition="replace",
        loader_file_format="parquet",
    )
    print(load_info)
