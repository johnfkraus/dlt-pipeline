# bronze_co1.py
from typing import Iterator, TypedDict
import dlt
import pandas as pd

class Co1Row(TypedDict):
    contact: str
    target: str
    person_a: str
    person_b: str
    date_of_first_interaction: str
    date_of_last_interaction: str
    additional_info: str
    dataset_name: str

@dlt.resource(
    name="co1_bronze",
    write_disposition="merge",
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
def co1_bronze(
    cursor: dlt.sources.incremental = dlt.sources.incremental(
        "date_of_first_interaction",
        initial_value="2000-01-01",
    ),
    path: str = "data/co1.csv",
) -> Iterator[Co1Row]:
    df = pd.read_csv(path, dtype=str).fillna("")
    for rec in df.to_dict(orient="records"):
        yield Co1Row(**rec)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bronze_co1",
        destination="postgres",
        dataset_name="bronze",
    )
    info = pipeline.run(co1_bronze())
    print(info)
