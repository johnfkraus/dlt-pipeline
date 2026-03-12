<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Using Python, Polars and PostgreSQL, I want to use something like df.with_columns(), pl.when(), and pl.otherwise to conditionally normalize a column of data using third party normalizing code.  If normalize1 returns None, it should try normalizer2.  If Normalizer2 returns none, it should try normalizer3, etc.  Finally it should try normalizer_mixed.  The code should return the result of any successful normalization, or None if no normalization returns a non-None value.

One of the normalizers has two parameters.
Please give me code for this.

You can wrap your third‑party normalizers in a single helper function and call it from a Polars `apply` expression.[^1][^2]

```python
import polars as pl

# Third‑party normalizers (examples)
# normalize1(x) -> str | None
# normalize2(x) -> str | None
# normalize3(x, extra) -> str | None
# normalize_mixed(x) -> str | None

EXTRA_PARAM_FOR_NORM3 = "some-extra-arg"

def try_normalizers(value: str | None) -> str | None:
    """
    Try each normalizer in order and return the first non‑None result.
    If all fail or value is None, return None.
    """
    if value is None:
        return None

    for fn, needs_extra in [
        (normalize1, False),
        (normalize2, False),
        (normalize3, True),   # this one needs the second parameter
        (normalize_mixed, False),
    ]:
        if needs_extra:
            result = fn(value, EXTRA_PARAM_FOR_NORM3)
        else:
            result = fn(value)

        if result is not None:
            return result

    return None


# Example Polars usage
df = pl.DataFrame(
    {
        "raw_value": ["abc", "DEF", None, "xyz"],
    }
)

df_normalized = df.with_columns(
    pl.col("raw_value")
      .apply(try_normalizers, return_dtype=pl.Utf8)
      .alias("normalized_value")
)

print(df_normalized)
```

If you want to push this into PostgreSQL via `COPY`, `write_database`, or similar, you can run this `with_columns` transformation first, then write the resulting `df_normalized` to your Postgres table.[^2]

Do any of your normalizers need access to other columns in the same row (besides the one being normalized)?
<span style="display:none">[^10][^11][^12][^13][^14][^15][^3][^4][^5][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://docs.pola.rs/user-guide/expressions/user-defined-python-functions/

[^2]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.with_columns.html

[^3]: https://github.com/pola-rs/polars/issues/22505

[^4]: https://docs.pola.rs/docs/python/dev/reference/expressions/api/polars.when.html

[^5]: https://stackoverflow.com/questions/76096409/conditional-assignment-to-multiple-columns-in-polars

[^6]: https://github.com/pola-rs/polars/issues/22939

[^7]: https://www.youtube.com/watch?v=3hwEUXWEqCY

[^8]: https://www.geeksforgeeks.org/python/how-to-add-a-column-to-a-polars-dataframe-using-withcolumns/

[^9]: https://typethepipe.com/vizs-and-tips/python-polars-suggest-efficient-expressions-lambda-function/

[^10]: https://stackoverflow.com/questions/78142482/create-columns-in-polars-with-when-then-otherwise-using-for-loop

[^11]: https://www.reddit.com/r/learnpython/comments/j4g0gs/how_do_i_use_filter_and_lambda_in_a_situation/

[^12]: https://www.emilyriederer.com/post/py-rgo-udf/

[^13]: https://stackoverflow.com/questions/74433918/apply-a-function-to-2-columns-in-polars

[^14]: https://docs.pola.rs/user-guide/expressions/aggregation/

[^15]: https://www.geeksforgeeks.org/python/how-to-apply-a-custom-function-in-polars-that-does-the-processing-row-by-row/

