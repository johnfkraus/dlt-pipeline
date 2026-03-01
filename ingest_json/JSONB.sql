CREATE SCHEMA ingest_json;

CREATE TABLE ingest_json.raw_import (doc jsonb);

DROP TABLE IF EXISTS ingest_json.my_data;

CREATE TABLE ingest_json.my_data (
  id      integer,
  name    text,
  age     integer,
  details jsonb
);

INSERT INTO INGEST_JSON.my_data (id, name, age, details)
SELECT (doc->>'id')::int                    AS id,
       doc->>'name'                         AS name,
       (doc->'details'->>'age')::int        AS age,
       doc->'details'                       AS details
FROM ingest_json.raw_import;

select * from ingest_json.my_data;



CREATE TABLE ingest_json.my_data (
  id     integer,
  name   text,
  details jsonb
);



INSERT INTO ingest_json.my_data (id, name, details)
SELECT (obj->>'id')::int        AS id,
       obj->>'name'             AS name,
       obj->'details'           AS details
FROM ingest_json.raw_import,
     jsonb_array_elements(doc) AS obj;



-- See some raw values
SELECT doc
FROM ingest_json.raw_import
LIMIT 5;

-- Check the JSON type per row
SELECT jsonb_typeof(doc) AS type, count(*)
FROM ingest_json.raw_import
GROUP BY jsonb_typeof(doc);


DO
$$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT quote_ident(nspname) AS schema_name
        FROM pg_namespace
        WHERE nspname LIKE 'bronze%'
          AND nspname NOT IN ('pg_catalog', 'information_schema')
    LOOP
        RAISE NOTICE 'Dropping schema: %', r.schema_name;
        EXECUTE format('DROP SCHEMA %s CASCADE;', r.schema_name);
    END LOOP;
END;
$$;

