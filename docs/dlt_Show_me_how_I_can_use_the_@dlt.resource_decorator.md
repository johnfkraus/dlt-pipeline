<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Show me how I can use the @dlt.resource decorator to provide type hints in the Python code.

You can combine `@dlt.resource` with normal Python type hints on the function signature and with schema hints on the decorator itself.[^1][^2][^3]

Below are a few focused examples you can adapt to your pipeline.

***

## 1. Strongly typed row model with `TypedDict`

```python
from typing import Iterable, Iterator, TypedDict
import dlt

class Co1Row(TypedDict):
    contact: str
    target: str
    person_a: str
    person_b: str
    date_of_first_interaction: str
    date_of_last_interaction: str
    additional_info: str
    dataset_name: str

@dlt.resource(name="c01_bronze", write_disposition="replace")
def c01_bronze_resource(rows: Iterable[Co1Row]) -> Iterator[Co1Row]:
    """Yield already-parsed c01 rows into dlt with type-checked structure."""
    for row in rows:
        yield row
```

Here:

- `Co1Row` documents the expected keys and value types.
- The function argument and return types are standard Python hints; tools and editors can validate them.[^4][^3]
- `@dlt.resource` wraps this function into a resource that dlt can load.[^2][^1]

You can then call:

```python
def extract_from_pandas() -> list[Co1Row]:
    # build a list[Co1Row] from a DataFrame, for example
    ...

rows = extract_from_pandas()
pipeline.run(c01_bronze_resource(rows))
```


***

## 2. Resource that reads and yields rows itself

You can also type the yielded objects directly:

```python
from typing import Iterator
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

@dlt.resource(name="co2_bronze", write_disposition="replace")
def co2_bronze_resource(path: str = "data/co2.csv") -> Iterator[Co2Row]:
    """Read co2 CSV and yield typed rows."""
    df = pd.read_csv(path, dtype=str).fillna("")
    for rec in df.to_dict(orient="records"):
        yield Co2Row(**rec)  # mypy/pyright will check keys
```

This gives you:

- Type checking on the function argument (`path`).
- Type checking on the yielded records via `Co2Row`.

***

## 3. Adding schema hints via the decorator (`type`, `primary_key`, etc.)

In addition to Python hints, you can tell dlt about the physical schema using `columns` in the decorator (or later via `apply_hints`).[^5][^3][^2]

```python
@dlt.resource(
    name="c01_bronze",
    write_disposition="replace",
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
    primary_key=None,
)
def c01_bronze_resource(rows: Iterable[Co1Row]) -> Iterator[Co1Row]:
    for row in rows:
        yield row
```

This keeps all columns as text in Postgres while still giving your editor strong Python typing for the in-memory structures.[^3][^2]

***

## 4. Applying hints after defining the resource

If you want to separate Python typing from dlt’s schema hints, you can use `apply_hints` after declaration.[^2][^5]

```python
@dlt.resource(name="co3_bronze", write_disposition="replace")
def co3_bronze_resource(rows: Iterable[Co2Row]) -> Iterator[Co2Row]:
    for row in rows:
        yield row

# Attach dlt-specific hints later
co3_bronze_resource.apply_hints(
    columns={
        "selector_a": {"data_type": "text"},
        "selector_b": {"data_type": "text"},
        "name_a": {"data_type": "text"},
        "name_b": {"data_type": "text"},
        "date_of_interaction": {"data_type": "text"},
        "additional_info": {"data_type": "text"},
        "comment": {"data_type": "text"},
        "dataset_name": {"data_type": "text"},
    }
)
```

The important point: regular Python type hints (on parameters and return type) are for your IDE and static checkers, while `@dlt.resource` arguments and `apply_hints` control how dlt interprets and stores that data.[^1][^3][^2]

If you tell me which of your pipeline functions you want to convert first, I can rewrite that exact function with `@dlt.resource` and full type hints.
<span style="display:none">[^10][^11][^12][^13][^14][^15][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://dlthub.com/docs/general-usage/resource

[^2]: https://raw.githubusercontent.com/dlt-hub/dlt/refs/heads/devel/docs/website/docs/general-usage/resource.md

[^3]: https://dlthub.com/docs/general-usage/schema

[^4]: https://dlthub.com/docs/api_reference/dlt/common/typing

[^5]: https://github.com/dlt-hub/dlt/issues/1647

[^6]: https://dlthub.com/docs/api_reference/dlt/extract/decorators

[^7]: https://molab.marimo.io/github/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_2_dlt_sources_and_resources_create_first_dlt_pipeline.py

[^8]: https://dev.to/pizofreude/study-notes-dlt-fundamentals-course-lesson-5-6-write-disposition-incremental-loading-how-dlt-429j

[^9]: https://dlthub.com/docs/api_reference/dlt/sources/rest_api/typing

[^10]: https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_1_custom_sources_restapi_source_and_restclient.ipynb

[^11]: https://dlthub.com/docs/general-usage/schema-contracts

[^12]: https://github.com/dagster-io/dagster/discussions/25019

[^13]: https://github.com/dlt-hub/dlt/issues/1221

[^14]: https://discuss.dagster.io/t/22688034/u0667dnc02y-how-can-i-use-the-dlt-assets-decorator-to-create

[^15]: https://www.datacamp.com/tutorial/python-dlt

