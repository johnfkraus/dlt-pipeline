# bronze_c01_polars2.py
from typing import Iterator, Dict, Any
import dlt
import polars as pl
from timer import  get_time

EXCEL_FILE_NAME = "c01_edge.xlsx"
SCHEMA_NAME = "bronze"
EXCEL_FILE_PATH = f"../data/xlsx/{EXCEL_FILE_NAME}"
PIPELINE_NAME = f"{SCHEMA_NAME}_{EXCEL_FILE_NAME.removesuffix('.xlsx')}"
RESOURCE_NAME = f"{EXCEL_FILE_NAME.removesuffix('.xlsx')}"  # TABLE name

@dlt.resource(
    name=RESOURCE_NAME,  # table name
    # write_disposition="append",  # or "replace"/"merge"
    write_disposition="replace",  # or "replace"/"merge"
)

def comms_bronze(
    excel_path: str = EXCEL_FILE_PATH,
    sheet_name: str | int | None = 0,
) -> Iterator[Dict[str, Any]]:
    """
    Load Excel into a Polars DataFrame and yield row dicts.
    Column names come from the Excel header row.
    """
    df = pl.read_excel(source=excel_path)  # , sheet_name=sheet_name)
    # df = df.fill_null("")  # avoid None/NaN

    for rec in df.to_dicts():
        yield rec


def build_text_column_hints(excel_path: str = EXCEL_FILE_PATH) -> dict:  # , sheet_name: str | int | None = 0) -> dict:
    """
    Inspect the Excel header and build a columns hint where
    every column is declared as TEXT for Postgres.
    """
    df = pl.read_excel(source=excel_path)  # polars picks the first sheet name automatically, sheet_name=sheet_name)
    columns = {}
    for col in df.columns:
        columns[col] = {"data_type": "text"}
    # Add dlt system columns if you want them explicitly typed
    columns["_dlt_id"] = {"data_type": "text"}
    columns["_dlt_load_id"] = {"data_type": "text"}
    return columns


@get_time
def main():
    excel_file_path: str = EXCEL_FILE_PATH  # f"data/xlsx/{EXCEL_FILE_NAME}"
    # 1) Build dynamic column hints from the Excel file
    cols_hint = build_text_column_hints(excel_file_path)  #, sheet_name=0)

    # 2) Apply them to the resource so dlt creates all columns as TEXT
    comms_bronze.apply_hints(columns=cols_hint)  # [web:54][web:190]

    # 3) Run the pipeline as usual
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="postgres",
        dataset_name=SCHEMA_NAME,  # "bronze",
        dev_mode=True
    )
    info = pipeline.run(comms_bronze(excel_path=excel_file_path, sheet_name=0))
    print(info)


if __name__ == "__main__":
    main()


