<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Using Python, Polars and PostgreSQL, I want to use something like df.with_columns(), pl.when(), and pl.otherwise to conditionally normalize a column of data using third party normalizing code.  If normalize1 returns None, it should try normalizer2.  If Normalizer2 returns none, it should try normalizer3, etc.  Finally it should try normalizer_mixed.  The code should return the result of any successful normalization, or None if no normalization returns a non-None value.  Please give me code for this.

You can wrap your third‑party normalizers in a small Python function and call it via a Polars UDF, then use `with_columns` to apply it to the target column.

### Example: chained normalizers with fallback

```python
import polars as pl

# your 3rd‑party normalizers; they should return either a normalized value or None
def normalizer1(x): ...
def normalizer2(x): ...
def normalizer3(x): ...
def normalizer_mixed(x): ...

def chained_normalizer(value):
    """Try each normalizer in order; return first non‑None, else None."""
    for fn in (normalizer1, normalizer2, normalizer3, normalizer_mixed):
        result = fn(value)
        if result is not None:
            return result
    return None

df = (
    df
    .with_columns(
        pl.col("raw_col")
        .map_elements(chained_normalizer, return_dtype=pl.String)  # or appropriate dtype
        .alias("normalized_col")
    )
)
```

Key points:

- `map_elements` calls `chained_normalizer` for each value in the column and builds a new column from the return values.[^1][^2]
- Inside `chained_normalizer`, you can keep all your fallback logic in regular Python, including any complex use of third‑party libraries.[^1]

If you specifically need to keep everything inside an expression chain (e.g., mixing with other `pl.when(...).then(...).otherwise(...)` logic), you can still use the UDF as the `then`/`otherwise` value:

```python
df = df.with_columns(
    pl.when(pl.col("raw_col").is_not_null())
      .then(pl.col("raw_col").map_elements(chained_normalizer, return_dtype=pl.String))
      .otherwise(None)
      .alias("normalized_col")
)
```

Would you like an example wired up end‑to‑end with a small sample DataFrame and dummy normalizer functions?
<span style="display:none">[^10][^11][^12][^13][^14][^15][^3][^4][^5][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://docs.pola.rs/user-guide/expressions/user-defined-python-functions/

[^2]: https://docs.pola.rs/api/python/version/0.20/reference/expressions/functions.html

[^3]: https://docs.pola.rs/docs/python/dev/reference/expressions/api/polars.when.html

[^4]: https://stackoverflow.com/questions/75401574/when-then-otherwise-in-python-polars

[^5]: https://github.com/pola-rs/polars/issues/17601

[^6]: https://www.emilyriederer.com/post/py-rgo-udf/

[^7]: https://pavel.pink/blog/polarify

[^8]: https://github.com/pola-rs/polars/issues/22939

[^9]: https://pola-rs.github.io/r-polars/vignettes/custom-functions.html

[^10]: https://www.youtube.com/watch?v=3hwEUXWEqCY

[^11]: https://stackoverflow.com/questions/76150718/how-to-apply-multiple-different-functions-to-columns-in-polars-groupby

[^12]: https://stackoverflow.com/questions/77279189/defining-a-custom-expression-containing-a-conditional

[^13]: https://www.geeksforgeeks.org/python/how-to-apply-a-custom-function-in-polars-that-does-the-processing-row-by-row/

[^14]: https://docs.pola.rs/api/python/dev/reference/api/polars.json_normalize.html

[^15]: https://docs.pola.rs/py-polars/html/reference/lazyframe/index.html

