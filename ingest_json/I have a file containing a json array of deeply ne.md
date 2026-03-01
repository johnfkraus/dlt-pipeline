<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# I have a file containing a json array of deeply nested json objects.  How can I ingest this file into postgres?

You have two main options: store the JSON “as‑is” in a jsonb column, or normalize it into relational tables. Here’s how to do both for a JSON **array** of nested objects.

***

## 1. Load the raw JSON array into Postgres

Assume your file is `data.json` and looks like:

```json
[
  { "id": 1, "name": "Alice", "details": { "age": 30 } },
  { "id": 2, "name": "Bob",   "details": { "age": 25 } }
]
```


### Option A: Load whole file into one row, then unpack

1. Create a staging table and a target table:
```sql
CREATE TABLE raw_import (doc jsonb);

CREATE TABLE my_data (
  id     integer,
  name   text,
  details jsonb
);
```

2. Load the file into the staging table (psql):
```sql
\copy raw_import(doc) FROM 'data.json';
```

This puts the entire JSON array into `raw_import.doc` as one jsonb value.[^1]

3. Expand the JSON array into rows in `my_data`:
```sql
INSERT INTO my_data (id, name, details)
SELECT (obj->>'id')::int        AS id,
       obj->>'name'             AS name,
       obj->'details'           AS details
FROM raw_import,
     jsonb_array_elements(doc) AS obj;
```

`jsonb_array_elements` iterates over the array and returns one row per element, which you can then dissect with `->` and `->>` into columns.[^2][^1]

***

## 2. Load JSON “lines” and then unpack

If you can pre‑convert your array file into one JSON object per line (using `jq`):

```bash
jq -c '.[]' data.json > data_lines.json
```

`data_lines.json` will look like:

```json
{"id":1,"name":"Alice","details":{"age":30}}
{"id":2,"name":"Bob","details":{"age":25}}
```

1. Table to hold each object as jsonb:
```sql
CREATE TABLE raw_import (doc jsonb);
```

2. Load each line as one row:
```sql
\copy raw_import(doc) FROM 'data_lines.json';
```

3. Insert into your target table:
```sql
CREATE TABLE my_data (
  id      integer,
  name    text,
  age     integer,
  details jsonb
);

INSERT INTO my_data (id, name, age, details)
SELECT (doc->>'id')::int                    AS id,
       doc->>'name'                         AS name,
       (doc->'details'->>'age')::int        AS age,
       doc->'details'                       AS details
FROM raw_import;
```

The chained `->` / `->>` operators let you reach deeply nested attributes (e.g. `doc->'details'->'address'->>'city'`).[^3][^2]

***

## 3. Using jsonb_populate_recordset for flat parts

If each element of your top‑level array matches a row type (flat or mostly flat), you can let Postgres map fields automatically:

```sql
CREATE TABLE people (
  id   integer,
  name text,
  age  integer
);

-- staging:
CREATE TABLE raw_import (doc jsonb);
\copy raw_import(doc) FROM 'data.json';

INSERT INTO people (id, name, age)
SELECT p.*
FROM raw_import r,
     jsonb_populate_recordset(NULL::people, r.doc) AS p;
```

This works when the top‑level objects have keys matching the `people` columns; nested bits can be left in extra jsonb columns or handled separately.[^4][^1]

***

## 4. Querying deeply nested JSON afterwards

If you keep some structure in jsonb columns, you can query nested fields directly:

```sql
-- Get rows where details.age >= 30
SELECT *
FROM my_data
WHERE (details->>'age')::int >= 30;

-- Get nested value via a path
SELECT details #> '{address,city}' AS city
FROM my_data;
```

Operators `->`, `->>`, `#>` and related JSON operators are designed for this style of nested querying.[^5][^3][^2]

***

If you share a small sample of your actual JSON (with sensitive data removed), I can sketch the exact CREATE TABLE and INSERT/SELECT statements tailored to your structure.
<span style="display:none">[^10][^11][^12][^13][^14][^15][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://stackoverflow.com/questions/39224382/how-can-i-import-a-json-file-into-postgresql

[^2]: https://www.postgresql.org/docs/current/datatype-json.html

[^3]: https://hashrocket.com/blog/posts/dealing-with-nested-json-objects-in-postgresql

[^4]: https://neon.com/postgresql/postgresql-json-functions/postgresql-jsonb_populate_recordset

[^5]: https://neon.com/guides/document-store

[^6]: https://www.youtube.com/watch?v=PhQ0JOSKVZM

[^7]: https://www.youtube.com/watch?v=NgIdO4FkC_A

[^8]: https://ftisiot.net/postgresqljson/load-json-data-with-postgresql-copy/

[^9]: https://hackernoon.com/how-to-query-deeply-nested-json-data-in-psql

[^10]: https://www.reddit.com/r/slavelabour/comments/19dq1rs/task_import_a_400mb_nested_json_file_into_a/

[^11]: https://docs.risingwave.com/sql/functions/json

[^12]: https://stackoverflow.com/questions/57925988/using-copy-to-import-a-json-file-into-a-postgresql-table

[^13]: https://www.reddit.com/r/PostgreSQL/comments/1hp82cd/loading_json_array_to_postgres/

[^14]: https://community.amazonquicksight.com/t/how-to-import-json-with-nested-array/3758

[^15]: https://stackoverflow.com/questions/29108179/jsonb-query-with-nested-objects-in-an-array

