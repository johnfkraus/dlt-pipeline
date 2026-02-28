# bronze_c01.py
from typing import Iterator, TypedDict
import dlt
import pandas as pd

class C01Row(TypedDict):
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
    # write_disposition="merge",
    write_disposition="replace",
    primary_key="date_of_first_interaction",  # or real key if you have one
    columns={
        "contact": {"data_type": "text"},
        "target": {"data_type": "text"},
        "person_a": {"data_type": "text"},
        "person_b": {"data_type": "text"},
        "date_of_first_interaction": {"data_type": "text"},
        "date_of_last_interaction": {"data_type": "text"},
        "additional_info": {"data_type": "text"},
        "dataset_name": {"data_type": "text"},
        # dlt system columns will be text/varchar in Postgres
        "_dlt_id": {"data_type": "text"},
        "_dlt_load_id": {"data_type": "text"},
    },
)
def c01_bronze(
    cursor: dlt.sources.incremental = dlt.sources.incremental(
        "date_of_first_interaction",
        initial_value="2000-01-01",
    ),
    csv_path: str = "data/c01.csv",
) -> Iterator[C01Row]:
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    for rec in df.to_dict(orient="records"):
        yield C01Row(**rec)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bronze_c01",
        destination="postgres",
        dataset_name="bronze",
        dev_mode=True
    )
    info = pipeline.run(c01_bronze())
    print(info)
