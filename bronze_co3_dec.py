# bronze_co3.py
from typing import Iterator, TypedDict
import dlt
import pandas as pd

class Co3Row(TypedDict):
    selector_a: str
    selector_b: str
    name_a: str
    name_b: str
    date_of_interaction: str
    additional_info: str
    comment: str
    dataset_name: str

@dlt.resource(
    name="co3_bronze",
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
def co3_bronze(path: str = "data/co3.csv") -> Iterator[Co3Row]:
    df = pd.read_csv(path, dtype=str).fillna("")
    for rec in df.to_dict(orient="records"):
        yield Co3Row(**rec)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bronze_co3",
        destination="postgres",
        dataset_name="bronze",
        full_refresh=True,
    )
    load_info = pipeline.run(co3_bronze())
    print(load_info)
