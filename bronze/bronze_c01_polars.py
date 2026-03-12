# bronze_c01_polars2.py
from typing import Iterator, Dict, Any
import dlt
import uuid
import polars as pl
from timer import  get_time
import sys
from pathlib import Path
# Get the parent directory and add it to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(parent_dir))
# Import the module
from print_db_adbc import print_db

EXCEL_FILE_NAME = "c01_edge.xlsx"
SHEET_NAME = "c01_edge"
SCHEMA_NAME = "bronze"
EXCEL_FILE_PATH = f"../data/xlsx/{EXCEL_FILE_NAME}"
PIPELINE_NAME = f"{SCHEMA_NAME}_{EXCEL_FILE_NAME.removesuffix('.xlsx')}"
RESOURCE_NAME = f"{EXCEL_FILE_NAME.removesuffix('.xlsx')}"  # TABLE name

@dlt.resource(
    name=RESOURCE_NAME,  # table name
    write_disposition="replace",  # "append"/"replace"/"merge"
)

def comms_bronze(
    excel_path: str = EXCEL_FILE_PATH,
    sheet_name: str | int | None = 0,
) -> Iterator[Dict[str, Any]]:
    """
    Load Excel into a Polars DataFrame and yield row dicts.
    Column names come from the Excel header row.
    """
    df = pl.read_excel(
        source=excel_path,
        sheet_name = SHEET_NAME,
        read_options = {"header_row": 2}
    )  # , sheet_name=sheet_name)
    # df = df.fill_null("")  # avoid None/NaN

    df = df.with_columns(
        pl.lit(None)
        .map_elements(lambda _: uuid.uuid4().hex, return_dtype=pl.Utf8)
        .alias("bronze_row_id")
    )

    print(f"{df.columns=}")

    for rec in df.to_dicts():
        yield rec


def build_text_column_hints(excel_path: str = EXCEL_FILE_PATH) -> dict:  # , sheet_name: str | int | None = 0) -> dict:
    """
    Inspect the Excel header and build a columns hint where
    every column is declared as TEXT for Postgres.
    """
    df = pl.read_excel(
        source=excel_path,
        sheet_name=SHEET_NAME,
        read_options={"header_row": 2}
    )  # polars picks the first sheet name automatically, sheet_name=sheet_name)
    columns = {}
    for col in df.columns:
        columns[col] = {"data_type": "text"}
    # Add dlt system columns if you want them explicitly typed
    columns["_dlt_id"] = {"data_type": "text"}
    columns["_dlt_load_id"] = {"data_type": "text"},
    columns["bronze_row_uuid"] = {"data_type": "text"}
    return columns


@get_time
def main():
    excel_file_path: str = EXCEL_FILE_PATH  # f"data/xlsx/{EXCEL_FILE_NAME}"
    # 1) Build dynamic column hints from the Excel file
    cols_hint = build_text_column_hints(excel_file_path)  #, sheet_name=0)

    # 2) Apply them to the resource so dlt creates all columns as TEXT
    # comms_bronze.apply_hints(columns=cols_hint)  # [web:54][web:190]

    # 3) Run the pipeline as usual
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="postgres",
        dataset_name=SCHEMA_NAME,  # "bronze",
        dev_mode=True
    )
    info = pipeline.run(comms_bronze(
        excel_path=excel_file_path,
        sheet_name=SHEET_NAME))
    # print(f"{info=}")

    schema_name = info.pipeline.dataset_name
    # print(f"{schema_name=}")
    print(f"{RESOURCE_NAME=}")
    print_db("comms", schema_name, RESOURCE_NAME)


if __name__ == "__main__":
    main()


