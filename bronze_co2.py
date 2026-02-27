# bronze_co2.py
import dlt
import pandas as pd

PIPELINE_NAME = "bronze_co2"
DATASET_NAME = "bronze"
TABLE_NAME = "co2_bronze"

def extract_co2():
    df = pd.read_excel("data/co2.xlsx", dtype=str)
    df = df.astype(str)
    return df.to_dict(orient="records")

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="postgres",
        dataset_name=DATASET_NAME,
        full_refresh=True,
    )

    data = extract_co2()
    load_info = pipeline.run(
        data,
        table_name=TABLE_NAME,
        write_disposition="replace",
        loader_file_format="parquet",
    )
    print(load_info)
