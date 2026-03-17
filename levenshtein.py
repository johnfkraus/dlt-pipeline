
def levenshtein(s1: str, s2: str | None) -> int:
    # Handle None like empty string if needed
    if s1 is None:
        s1 = ""
    if s2 is None:
        s2 = ""

    m, n = len(s1), len(s2)

    # dp[i][j] = distance between s1[:i] and s2[:j]
    dp = [[0] * (n + 1) for _ in range(m + 1)]

    # Base cases: distance from/to empty string
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j

    # Fill the table
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            cost = 0 if s1[i - 1] == s2[j - 1] else 1
            dp[i][j] = min(
                dp[i - 1][j] + 1,      # deletion
                dp[i][j - 1] + 1,      # insertion
                dp[i - 1][j - 1] + cost  # substitution
            )

    print(dp)
    return dp[m][n]


# Example
if __name__ == "__main__":
    print(f"{levenshtein('kitten', 'sitting')=}")  # 3
    print(f"{levenshtein('sitting', 'kitten')=}")  # 3
    print(f"{levenshtein('1234567890', None)=}")  # 10

    print(f"{levenshtein('fast', 'fart')=}")
