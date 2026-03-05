# bronze_c01.py
from typing import Iterator, TypedDict
from uuid import uuid4
import dlt
import pandas as pd


class C01Row(TypedDict):
    bronze_row_uuid: str
    contact: str
    target: str
    person_a: str
    person_b: str
    date_of_first_interaction: str
    date_of_last_interaction: str
    additional_info: str
    dataset_name: str


@dlt.resource(
    name="c01_bronze",
    write_disposition="replace",
    primary_key="bronze_row_uuid",  # UUID PK
    columns={
        # Generated UUID primary key (generated in Python)
        "bronze_row_uuid": {
            "data_type": "uuid",
            "primary_key": True,
            "nullable": False,
        },
        # Excel columns - all TEXT
        "contact": {"data_type": "text"},
        "target": {"data_type": "text"},
        "person_a": {"data_type": "text"},
        "person_b": {"data_type": "text"},
        "date_of_first_interaction": {"data_type": "text"},
        "date_of_last_interaction": {"data_type": "text"},
        "additional_info": {"data_type": "text"},
        "dataset_name": {"data_type": "text"},
        "_dlt_id": {"data_type": "text"},
        "_dlt_load_id": {"data_type": "text"},
    },
)
def c01_bronze(
    cursor: dlt.sources.incremental = dlt.sources.incremental(
        "date_of_first_interaction",
        initial_value="2000-01-01",
    ),
    excel_path: str = "../data/xlsx/c01.xlsx",
) -> Iterator[C01Row]:
    # Read Excel spreadsheet
    df = pd.read_excel(excel_path, dtype=str).fillna("")

    for rec in df.to_dict(orient="records"):
        # Generate UUID for each row before yielding
        bronze_row_uuid = str(uuid4())  # Python UUID → string

        yield C01Row(
            bronze_row_uuid=bronze_row_uuid,
            **rec,  # unpack Excel columns
        )


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bronze_c01",
        destination="postgres",
        dataset_name="bronze",
        dev_mode=True
    )
    info = pipeline.run(c01_bronze())
    print(info)
