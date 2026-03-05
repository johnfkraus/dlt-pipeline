# pip install "dlt[parquet]"
import dlt
import pandas as pd
import uuid
from pathlib import Path

# Fixed spreadsheet path and details
spreadsheet_path = Path("../data/xlsx/c01b.xlsx")
table_name = "c01b"


@dlt.resource(
    # name=table_name,
    table_name=lambda item: "c01b",
    write_disposition=lambda item: "replace",
    # write_disposition="replace",
    columns=lambda item: {col: {"data_type": "text"} for col in item.keys()},
)
def comms_data():
    """Load c01b.xlsx sheet 'c01b' with headers on row 3"""
    df = pd.read_excel(
        spreadsheet_path,
        sheet_name="c01b",
        header=2,  # Headers on row 3 (0-indexed)
        engine="openpyxl"
    )

    # Add UUID column first (stable position)
    df.insert(0, "bronze_row_uuid", [str(uuid.uuid4()) for _ in range(len(df))])

    # Convert ALL columns to string/text type
    for col in df.columns:
        df[col] = df[col].astype(str)

    yield df


# Pipeline to bronze.comms.c01b
pipeline = dlt.pipeline(
    pipeline_name="c01b_to_bronze",
    destination="postgres",
    dataset_name="comms",  # Database/schema name
    dev_mode=True
)

# Load to bronze.c01b table
info = pipeline.run(comms_data())
print(info)
print(f"Loaded to comms.bronze.{table_name}")
