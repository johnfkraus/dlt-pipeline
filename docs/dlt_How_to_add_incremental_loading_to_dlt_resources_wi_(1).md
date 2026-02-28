<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# How to add incremental loading to dlt resources with decorators

You add incremental loading to a `@dlt.resource` by introducing a special parameter of type `dlt.sources.incremental` (or defaulting to an `incremental(...)` object) and using it as a cursor filter.[^1][^2][^3][^4]

Below are small, focused patterns you can plug into your current resources.

***

## 1. Cursor-based incremental with a timestamp column

This is the most common approach: only load rows where `updated_at` (or similar) is after the last run’s maximum.[^3][^1]

```python
from typing import Iterator, TypedDict
import dlt
import pandas as pd

class MyRow(TypedDict):
    id: int
    value: str
    updated_at: str   # ISO 8601 string in source

@dlt.resource(
    name="my_table",
    write_disposition="merge",  # typical for incremental loads
    primary_key="id",
)
def my_table_resource(
    updated: dlt.sources.incremental = dlt.sources.incremental(
        "updated_at",           # cursor field in the data
        initial_value="2024-01-01T00:00:00",
    )
) -> Iterator[MyRow]:
    """
    Load only rows with updated_at > last stored cursor.
    dlt manages state for 'updated' across runs.
    """
    df = pd.read_csv("data/my_table.csv", dtype=str).fillna("")
    # Let dlt filter by cursor: just yield everything, incremental will keep new ones.
    for rec in df.to_dict(orient="records"):
        yield rec  # incremental wrapper inspects 'updated_at'
```

Key points:

- The parameter `updated` is of incremental type and wired to the `updated_at` cursor.[^2][^4]
- dlt tracks the max cursor in pipeline state and filters old records in subsequent runs.[^1][^3]

***

## 2. Applying incremental to your existing bronze `c01_bronze` (cursor = date_of_first_interaction)

For your pipeline, suppose you want to only load rows where `date_of_first_interaction` is newer than last run.[^3][^1]

```python
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
    name="c01_bronze",
    write_disposition="merge",           # or 'append', but merge is typical with PK
    primary_key="date_of_first_interaction",  # or a real business key if you have one
    columns={
        "contact": {"data_type": "text"},
        "target": {"data_type": "text"},
        "person_a": {"data_type": "text"},
        "person_b": {"data_type": "text"},
        "date_of_first_interaction": {"data_type": "text"},
        "date_of_last_interaction": {"data_type": "text"},
        "additional_info": {"data_type": "text"},
        "dataset_name": {"data_type": "text"},
    },
)
def c01_bronze(
    cursor: dlt.sources.incremental = dlt.sources.incremental(
        "date_of_first_interaction",
        initial_value="2000-01-01",
    )
) -> Iterator[Co1Row]:
    df = pd.read_csv("data/c01.csv", dtype=str).fillna("")
    for rec in df.to_dict(orient="records"):
        # just yield; incremental wrapper filters based on 'date_of_first_interaction'
        yield Co1Row(**rec)
```

Usage is the same as before:

```python
pipeline = dlt.pipeline(
    pipeline_name="bronze_c01",
    destination="postgres",
    dataset_name="bronze",
)

# first run: loads all rows with date_of_first_interaction >= 2000-01-01
pipeline.run(c01_bronze)

# later runs: only new rows with larger date_of_first_interaction are loaded
pipeline.run(c01_bronze)
```


***

## 3. Overriding the cursor window at runtime

You can override `initial_value` or add `end_value` when you call the resource to restrict the range loaded.[^5][^6][^2]

```python
# Override just for this run:
pipeline.run(
    c01_bronze(
        cursor=dlt.sources.incremental(
            initial_value="2024-01-01",  # override, e.g. for backfill
            end_value="2024-12-31",
        )
    )
)
```

dlt merges the override with the original `incremental` definition.[^4][^2]

***

## 4. ID-based incremental (monotonic integer cursor)

If your source has a strictly increasing ID, you can use that instead of a date.[^2][^1]

```python
@dlt.resource(
    name="events",
    write_disposition="merge",
    primary_key="id",
)
def events(
    id_after: dlt.sources.incremental = dlt.sources.incremental(
        "id", initial_value=0
    )
):
    # Imagine this is reading from an API that you can filter with `id_after.start_value`
    start = id_after.start_value  # last max id or initial_value on first run
    # fetch only records with id > start
    for i in range(start + 1, start + 101):
        yield {"id": i, "payload": f"row-{i}"}
```

Here:

- The incremental object exposes `.start_value`, which is the point to start from.[^6][^2]
- The resource only generates newer IDs, so each run is naturally incremental.

***

## 5. Adding incremental to your `co2_bronze` and `co3_bronze`

Assuming `date_of_interaction` is your cursor:

```python
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
    write_disposition="merge",
    primary_key=("selector_a", "selector_b", "date_of_interaction"),
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
def co2_bronze(
    cursor: dlt.sources.incremental = dlt.sources.incremental(
        "date_of_interaction",
        initial_value="2000-01-01",
    )
) -> Iterator[Co2Row]:
    df = pd.read_csv("data/co2.csv", dtype=str).fillna("")
    for rec in df.to_dict(orient="records"):
        yield Co2Row(**rec)
```

You can copy that pattern for `co3_bronze` as well.

***

If you tell me which cursor columns you plan to use in each table (`date_of_first_interaction`, `date_of_interaction`, or an ID), I can rewrite your full bronze resource code with concrete incremental configs for each one.
<span style="display:none">[^10][^11][^12][^13][^14][^15][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://dlthub.com/docs/general-usage/incremental-loading

[^2]: https://dlthub.com/docs/api_reference/dlt/extract/incremental/__init__

[^3]: https://dlthub.com/docs/general-usage/incremental/cursor

[^4]: https://dlthub.com/docs/api_reference/extract/incremental/__init__/

[^5]: https://github.com/dlt-hub/dlt/issues/2735

[^6]: https://molab.marimo.io/github/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_5_write_disposition_and_incremental_loading.py

[^7]: https://dev.to/pizofreude/study-notes-dlt-fundamentals-course-lesson-5-6-write-disposition-incremental-loading-how-dlt-429j

[^8]: https://dlthub.com/docs/general-usage/resource

[^9]: https://www.youtube.com/watch?v=Tws7C2H2k7Q

[^10]: https://www.youtube.com/watch?v=ehaEky5p2Ao

[^11]: https://www.youtube.com/watch?v=2AUqv0ojwm0

[^12]: https://github.com/dlt-hub/cursor-dlt-example

[^13]: https://raw.githubusercontent.com/dlt-hub/dlt/refs/heads/devel/docs/website/docs/general-usage/resource.md

[^14]: https://github.com/dlt-hub/dlt_demos/blob/main/spotlight_demo.ipynb

[^15]: https://github.com/dlt-hub/dlt/issues/2751

