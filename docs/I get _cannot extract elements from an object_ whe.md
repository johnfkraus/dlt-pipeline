<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# I get "cannot extract elements from an object" when I run this:

Help!
INSERT INTO my_data (id, name, details)
SELECT (obj->>'id')::int        AS id,
obj->>'name'             AS name,
obj->'details'           AS details
FROM raw_import,
jsonb_array_elements(doc) AS obj;

That error means `jsonb_array_elements(doc)` is being given a JSON **object or scalar**, not a JSON **array**. In other words, at least one `raw_import.doc` row is not an array like `[...]`.[^1][^2]

### 1. Verify what is actually in `raw_import.doc`

Run these checks:

```sql
-- See some raw values
SELECT doc
FROM raw_import
LIMIT 5;

-- Check the JSON type per row
SELECT jsonb_typeof(doc) AS type, count(*)
FROM raw_import
GROUP BY jsonb_typeof(doc);
```

You should see `array` for this query to work; if you see `object` or `string`, that explains the error.[^3][^1]

If only some rows are arrays, you can filter:

```sql
SELECT (obj->>'id')::int        AS id,
       obj->>'name'             AS name,
       obj->'details'           AS details
FROM raw_import,
     jsonb_array_elements(doc) AS obj
WHERE jsonb_typeof(doc) = 'array';
```


### 2. Common situation: the whole file got stored as one object, not an array

If your file is an array at the top level, but you used something that wrapped it (or `COPY` read it differently), `doc` might look like:

```json
{"data": [ { ... }, { ... } ]}
```

In that case you must extract the array field:

```sql
SELECT (obj->>'id')::int        AS id,
       obj->>'name'             AS name,
       obj->'details'           AS details
FROM raw_import,
     jsonb_array_elements(doc->'data') AS obj;
```

Notice `doc->'data'` instead of `doc`.[^4][^2]

### 3. Common situation: the array is stored as a JSON *string*

Sometimes the array is inside quotes:

```json
"[{ \"id\": 1, \"name\": \"Alice\" }]"
```

Then `jsonb_typeof(doc)` will be `string`, and `jsonb_array_elements(doc)` fails. You need one more cast:[^5][^1]

```sql
SELECT (obj->>'id')::int        AS id,
       obj->>'name'             AS name,
       obj->'details'           AS details
FROM raw_import,
     jsonb_array_elements(doc::jsonb) AS obj;
```

If `doc` is a JSON string that itself contains JSON, you may need:

```sql
jsonb_array_elements(doc::text::jsonb)
```

depending on your column type.[^1][^3]

### 4. If your top level is a single object, not an array

If each `doc` is like:

```json
{ "id": 1, "name": "Alice", "details": { ... } }
```

then you **do not** need `jsonb_array_elements` at all; just use:

```sql
INSERT INTO my_data (id, name, details)
SELECT (doc->>'id')::int        AS id,
       doc->>'name'             AS name,
       doc->'details'           AS details
FROM raw_import;
```

`jsonb_array_elements` is only for when `doc` is a JSON array `[ {...}, {...} ]`.[^2][^6]

***

If you paste one actual `doc` value (or `SELECT doc FROM raw_import LIMIT 1;`), I can give the exact fixed `SELECT` you need.
<span style="display:none">[^10][^11][^12][^13][^14][^15][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://stackoverflow.com/questions/75382194/postgres-jsonb-array-elements-returns-cannot-extract-elements-from-a-scalar

[^2]: https://www.postgresql.org/docs/current/functions-json.html

[^3]: https://stackoverflow.com/questions/39236893/extract-json-array-from-postgres-table-gives-error-cannot-extract-elements-from

[^4]: https://popsql.com/learn-sql/postgresql/how-to-query-a-json-column-in-postgresql

[^5]: https://forum.reportserver.net/viewtopic.php?id=2656

[^6]: https://neon.com/postgresql/postgresql-json-functions/postgresql-jsonb_array_elements

[^7]: https://www.softrams.com/post/processing-postgresql-json-and-jsonb-array-elements

[^8]: https://neon.com/docs/functions/jsonb_array_elements

[^9]: https://stackoverflow.com/questions/57346900/error-while-getting-particular-object-from-jsonb-in-postgresql

[^10]: https://community.powerplatform.com/forums/thread/details/?threadid=3717bca3-b171-4407-9cd4-084b00975b09

[^11]: https://www.reddit.com/r/PostgreSQL/comments/oh1l1m/help_extracting_a_flatten_recordset_from_a_jsonb/

[^12]: https://community.n8n.io/t/issue-inserting-json-array-into-postgres-jsonb-column/79810

[^13]: https://www.youtube.com/watch?v=442jmNrSi50

[^14]: https://www.reddit.com/r/PostgreSQL/comments/11hvixl/help_using_jsonb_in_function/

[^15]: https://www.reddit.com/r/MicrosoftFlow/comments/142ib1o/expected_array_but_got_object/

