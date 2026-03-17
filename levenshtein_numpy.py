import numpy as np

def print_matrix(matrix, s1, s2, i_highlight=None, j_highlight=None):
    """
    Pretty-print the current DP matrix.
    Optionally highlight cell (i_highlight, j_highlight) with brackets.
    """
    m, n = matrix.shape
    # Header row
    header = ["    "]  # top-left corner (empty)
    header.append("   ")  # for s1 row labels
    for ch in s2:
        header.append(f"  {ch}")
    print("".join(header))

    for i in range(m):
        # Row label: first row is empty, then characters of s1
        if i == 0:
            row_label = "   "
        else:
            row_label = f" {s1[i-1]} "
        row_str = [row_label]
        for j in range(n):
            val = int(matrix[i, j])
            if i_highlight is not None and j_highlight is not None and i == i_highlight and j == j_highlight:
                # Highlight current cell
                cell = f"[{val:2d}]"
            else:
                cell = f" {val:2d} "
            row_str.append(cell)
        print("".join(row_str))
    print("\n" + "-" * 40 + "\n")


def levenshtein_numpy_verbose(s1, s2):
    """
    Compute Levenshtein distance using NumPy and
    print the matrix after each update.
    """
    m, n = len(s1), len(s2)

    # DP matrix of size (m+1) x (n+1)
    dp = np.zeros((m + 1, n + 1), dtype=int)

    # Initialize base cases
    dp[:, 0] = np.arange(m + 1)
    dp[0, :] = np.arange(n + 1)

    print("Initial matrix:")
    print_matrix(dp, s1, s2)

    # Fill the matrix
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            cost = 0 if s1[i - 1] == s2[j - 1] else 1

            deletion = dp[i - 1, j] + 1
            insertion = dp[i, j - 1] + 1
            substitution = dp[i - 1, j - 1] + cost

            dp[i, j] = min(deletion, insertion, substitution)

            print(f"After setting cell ({i}, {j}) for '{s1[:i]}' -> '{s2[:j]}'")
            print_matrix(dp, s1, s2, i_highlight=i, j_highlight=j)

    distance = int(dp[m, n])
    print(f"Final Levenshtein distance between '{s1}' and '{s2}': {distance}")
    return distance


if __name__ == "__main__":
    s1 = "kitten"
    s2 = "sitting"
    levenshtein_numpy_verbose(s1, s2)
