<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# How to disable staging schema in DLT pipelines

You can’t fully “turn off” the staging dataset for write modes that need it (especially `merge` and many `replace` cases), but you can avoid it by using `append` and otherwise only **rename or centralize** the staging schema.[^1][^2][^3][^4]

### 1. When staging is created (and when it isn’t)

dlt creates a staging **schema** (dataset) when the write disposition requires a safe intermediate area:[^2][^3][^4][^1]

- `write_disposition="merge"` → uses a staging dataset.
- `write_disposition="replace"` → uses staging for many destinations.
- `write_disposition="append"` → typically loads directly, no staging schema.

So the only way to truly “disable” staging is to avoid `merge`/`replace` and use `append` (or a destination/strategy that doesn’t need staging).[^5][^3][^4][^2]

Example: change your resource to append-only:

```python
@dlt.resource(
    name="co1_bronze",
    write_disposition="append",  # no staging schema for most destinations
    columns={...},
)
def co1_bronze(...):
    ...
```

If you were using `merge` for incremental upserts, switching to `append` means you lose dedup/merge behavior and have to handle that later (e.g., in SQL in silver).[^3][^4][^5]

### 2. If you must use merge/replace: only rename or centralize staging

If you still want merge semantics, you can’t disable staging, but you can:[^6][^7][^1][^2]

**a) Change the naming pattern in `.dlt/config.toml`:**

```toml
# .dlt/config.toml
[destination.postgres]
# single shared staging schema for all datasets
staging_dataset_name_layout = "_dlt_staging"
```

Now all staging tables go into schema `_dlt_staging` instead of `<dataset>_staging`.[^1][^6]

**b) Or use a per-dataset prefix:**

```toml
[destination.postgres]
staging_dataset_name_layout = "staging_%s"
# bronze -> staging_bronze
```

This doesn’t disable staging, but makes schemas easier to ignore or clean up.[^7][^6][^1]

### 3. Recommended approach for you

Given you’re building bronze/silver/gold and likely want incremental or merge behavior in bronze:

- For **bronze** (ingest): keep `merge` (or `replace`) and accept staging, but centralize it with `staging_dataset_name_layout="_dlt_staging"`.[^2][^1]
- For **silver/gold**: you’re already using psycopg2; those scripts don’t involve dlt, so no staging is created there.

So: you can’t truly disable the staging schema when using merge-style loads, but you can avoid it with `append`, or otherwise confine it to a single known schema and ignore it.[^4][^3][^1][^2]
<span style="display:none">[^10][^11][^12][^13][^14][^15][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://dlthub.com/docs/dlt-ecosystem/staging

[^2]: https://dlthub.com/docs/general-usage/destination-tables

[^3]: https://dlthub.com/docs/general-usage/incremental-loading

[^4]: https://dlthub.com/docs/general-usage/merge-loading

[^5]: https://dev.to/pizofreude/study-notes-dlt-fundamentals-course-lesson-5-6-write-disposition-incremental-loading-how-dlt-429j

[^6]: https://github.com/dlt-hub/dlt/issues/919

[^7]: https://github.com/dlt-hub/dlt/issues/1519

[^8]: https://community.databricks.com/t5/data-engineering/delta-live-tables-dynamic-schema/td-p/57626

[^9]: https://www.youtube.com/watch?v=9HWykQd0gO4

[^10]: https://github.com/dlt-hub/dlt/issues/2534

[^11]: https://dlthub.com/docs/api_reference/dlt/pipeline

[^12]: https://molab.marimo.io/github/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_5_write_disposition_and_incremental_loading.py

[^13]: https://dlthub.com/docs/reference

[^14]: https://dlthub.com/docs/general-usage/schema

[^15]: https://tessl.io/registry/tessl/pypi-dlt/1.19.0

