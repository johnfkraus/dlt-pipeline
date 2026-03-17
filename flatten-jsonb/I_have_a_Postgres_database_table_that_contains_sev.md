<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# I have a Postgres database table that contains several text columns (name_a, name_b, date, phone_a, phone_b) plus a jsonb-type column named 'other'.  I want SQL to flatten the 'other' jsonb column such that each unique key in the set of all jsonb keys becomes a column name in the generated table, with the values from the jsonb (if any) contained under that column for each row.  The generated table will contain the original text columns plus a new column for each unique key found in the jsonb.  Not all rows have the same keys in the jsonb column.  if there is no matching key and value in the jsonb for a given row, the value in the new column should be NULL.  There are about a dozen different unique keys in the jsonb data.  Please give me SQL for this as well as a sample CSV file that I can load in to create a table in postgres for testing.

You can do this in two steps: first discover the keys, then build a SELECT that projects each key as a column. Below is a practical pattern and a small CSV you can load for testing.

## 1. Example test table and CSV

Assume a table:

```sql
CREATE TABLE contacts (
    name_a  text,
    name_b  text,
    date    date,
    phone_a text,
    phone_b text,
    other   jsonb
);
```

Sample CSV (save as `contacts.csv`):

```csv
name_a,name_b,date,phone_a,phone_b,other
Alice,Smith,2025-01-01,111-111-1111,222-222-2222,"{""age"": 30, ""city"": ""New York"", ""vip"": true}"
Bob,Jones,2025-01-02,333-333-3333,,"{""age"": 45, ""city"": ""Boston""}"
Carol,Lee,2025-01-03,,444-444-4444,"{""age"": 28, ""vip"": false, ""preferred_contact"": ""email""}"
Dave,Khan,2025-01-04,555-555-5555,666-666-6666,"{""city"": ""Chicago""}"
Eve,Brown,2025-01-05,,, "{}"
```

Load into Postgres:

```sql
COPY contacts (name_a, name_b, date, phone_a, phone_b, other)
FROM '/absolute/path/to/contacts.csv'
WITH (FORMAT csv, HEADER true);
```


## 2. Get the list of JSONB keys

This query lists all distinct top‑level keys in `other`:

```sql
SELECT DISTINCT jsonb_object_keys(other) AS key
FROM contacts
ORDER BY key;
```

This uses `jsonb_object_keys` to expand object keys and `DISTINCT` to get the unique set for the whole column.[^1]

On the sample data, it will return:

- age
- city
- preferred_contact
- vip


## 3. Generate a flattened view (for known keys)

Once you know the keys, you can write a `SELECT` (or a view) that projects each key as a column. Values will be `NULL` where the key is absent.

Example:

```sql
CREATE OR REPLACE VIEW contacts_flat AS
SELECT
    name_a,
    name_b,
    date,
    phone_a,
    phone_b,
    other->>'age'                AS age,
    other->>'city'               AS city,
    other->>'preferred_contact'  AS preferred_contact,
    other->>'vip'                AS vip
FROM contacts;
```

Notes:

- `->>` returns text; if you want proper types, cast, e.g. `other->>'age'::int` is invalid, you need `(other->>'age')::int`.[^2][^3]
- Missing keys simply yield `NULL`, which matches your requirement.[^3]

If you want a physical table instead of a view:

```sql
CREATE TABLE contacts_flat AS
SELECT
    name_a,
    name_b,
    date,
    phone_a,
    phone_b,
    (other->>'age')::int          AS age,
    other->>'city'                AS city,
    other->>'preferred_contact'   AS preferred_contact,
    (other->>'vip')::boolean      AS vip
FROM contacts;
```

You would need to regenerate this table (or use `INSERT ... SELECT` / `REFRESH MATERIALIZED VIEW`) when new rows or new JSON keys appear.

## 4. Optional: helper to auto-generate the SELECT

If you want Postgres to output the SQL for the flattening step (so you don’t type the key list by hand), you can use something like this:

```sql
WITH keys AS (
  SELECT DISTINCT jsonb_object_keys(other) AS key
  FROM contacts
)
SELECT
  'SELECT name_a, name_b, date, phone_a, phone_b,' ||
  string_agg(
    format(' other->>%L AS %I', key, key),
    ','
  ) ||
  ' FROM contacts;'
AS generated_sql
FROM keys;
```

Run that once, copy the generated SQL, and use it to define your view or table. This pattern relies on the same `jsonb_object_keys` and `->>` operator described above.[^1][^3]

Do you need the flattened columns to be strongly typed (int, boolean, etc.), or is having everything as text acceptable for your use case?
<span style="display:none">[^10][^11][^12][^13][^14][^15][^4][^5][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://neon.com/postgresql/postgresql-json-functions/postgresql-jsonb_object_keys

[^2]: https://www.postgresql.org/docs/current/datatype-json.html

[^3]: https://neon.com/postgresql/postgresql-json-functions/postgresql-jsonb_each_text

[^4]: https://stackoverflow.com/questions/79134240/extract-values-from-jsonb-column-with-dynamic-keys

[^5]: https://dbschema.com/blog/postgresql/jsonb-in-postgresql/

[^6]: https://www.tigerdata.com/learn/how-to-query-jsonb-in-postgresql

[^7]: https://bender.io/2016/09/18/dynamic-pivot-tables-with-json-and-postgresql/

[^8]: https://www.enterprisedb.com/blog/postgresql-anti-patterns-unnecessary-jsonhstore-dynamic-columns

[^9]: https://ryanguill.com/postgresql/sql/2022/05/15/postgres-pivot-table-using-json.html

[^10]: https://stackoverflow.com/questions/49051959/postgresql-crosstab-simple-example

[^11]: https://mirakl.tech/how-jsonb-can-improve-your-postgresql-queries-90f6ed0c2f92

[^12]: https://www.reddit.com/r/PostgreSQL/comments/q83kh7/transpose_rows_to_columns_after_using_json_each/

[^13]: https://www.sqlshack.com/how-to-use-the-crosstab-function-in-postgresql/

[^14]: https://tenmilesquare.com/resources/technology-strategy-innovation/managing-complex-data-with-json-in-postgresql-part-2/

[^15]: https://stackoverflow.com/questions/36141388/how-can-i-get-all-keys-from-a-json-column-in-postgres

