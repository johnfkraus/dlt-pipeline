import pandas as pd
import numpy as np

def copy_down_n_times(df: pd.DataFrame,
                      column: str,
                      substring: str,
                      n: int) -> pd.DataFrame:
    """
    For each *original* row in `column` that contains `substring`,
    copy that cell's value into the next `n` rows in the same column,
    but only into rows where the cell is currently null (NaN).
    """
    result = df.copy()

    # Work off the original Series so new filled values don't become new sources
    col_original = df[column]

    # Indices of original rows that contain the substring
    source_indices = [
        i for i, val in col_original.items()
        if isinstance(val, str) and substring in val
    ]

    for i in source_indices:
        value = col_original.iloc[i]
        # Fill only the next n rows, if they are null in the *current* result
        for j in range(1, n + 1):
            idx = i + j
            if idx >= len(result):
                break
            if pd.isna(result.iat[idx, result.columns.get_loc(column)]):
                result.iat[idx, result.columns.get_loc(column)] = value

    return result

def get_dataframe():
    df = pd.DataFrame({
        "other": [
            np.nan,
            "Bananas are delicious",
            np.nan,
            np.nan,
            np.nan,
            "Apples are great",
            np.nan,
            np.nan,
            "Farts smell bad",
            np.nan,
            np.nan,
            np.nan,
            np.nan,
        ]
    })
    return df


def main():

            df = get_dataframe()

            print(f"{df=}")
            out = copy_down_n_times(df, column="other", substring="Bananas", n=2)
            print(f"{out=}")

            out = copy_down_n_times(out, column="other", substring="Farts", n=3)
            print(f"{out=}")


if __name__ == "__main__":
    main()