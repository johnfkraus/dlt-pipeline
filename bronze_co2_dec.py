# bronze_co2.py
from typing import Iterator, TypedDict
import dlt
import pandas as pd

class Co2Row(TypedDict):
    selector_a: str
    selector_b: str
    name_a: str
    name_b: str
    date_of_interaction: str
    additional_info: str
    comment: str
    dataset_name: str

@dlt.resource(
    name="co2_bronze",
    write_disposition="replace",
    columns={
        "selector_a": {"data_type": "text"},
        "selector_b": {"data_type": "text"},
        "name_a": {"data_type": "text"},
        "name_b": {"data_type": "text"},
        "date_of_interaction": {"data_type": "text"},
        "additional_info": {"data_type": "text"},
        "comment": {"data_type": "text"},
        "dataset_name": {"data_type": "text"},
    },
)
def co2_bronze(path: str = "data/co2.csv") -> Iterator[Co2Row]:
    df = pd.read_csv(path, dtype=str).fillna("")
    for rec in df.to_dict(orient="records"):
        yield Co2Row(**rec)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bronze_co2",
        destination="postgres",
        dataset_name="bronze",
        full_refresh=True,
    )
    load_info = pipeline.run(co2_bronze())
    print(load_info)
