CREATE SCHEMA ingest_json;

CREATE TABLE ingest_json.raw_import (doc jsonb);

CREATE TABLE my_data (
  id     integer,
  name   text,
  details jsonb
);


-- in psql
\copy raw_import(doc) FROM 'data.json';


INSERT INTO my_data (id, name, details)
SELECT (obj->>'id')::int        AS id,
       obj->>'name'             AS name,
       obj->'details'           AS details
FROM raw_import,
     jsonb_array_elements(doc) AS obj;


-- METHOD 2

jq -c '.[]' data.json > data_lines.json

