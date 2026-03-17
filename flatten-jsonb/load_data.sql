COPY contacts (name_a, name_b, date, phone_a, phone_b, other)
FROM '/Users/blauerbock/workspaces/dlt-pipeline/flatten-jsonb/contacts.csv'
WITH (FORMAT csv, HEADER true);


\copy public.contacts (name_a, name_b, date, phone_a, phone_b, other)
FROM '/Users/blauerbock/workspaces/dlt-pipeline/flatten-jsonb/contacts.csv'
WITH (FORMAT csv, HEADER true);

psql "postgresql://postgres:postgres@localhost/comms" \
  -c "\copy contacts (name_a, name_b, date, phone_a, phone_b, other) \
      FROM '/Users/blauerbock/workspaces/dlt-pipeline/flatten-jsonb/contacts.csv' \
      WITH (format csv, header true);"

I have Postgres running in a container on my Mac.
I want to load a CSV file into the public.contacts table.
The CSV file is here: '/Users/blauerbock/workspaces/dlt-pipeline/flatten-jsonb/contacts.csv'


docker run --name postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -v /Users/blauerbock/workspaces/dlt-pipeline/flatten-jsonb:/import \
  postgres



from mac terminal command line:

psql -h localhost -p 5432 -U postgres -d comms \
  -c "\copy public.contacts (name_a, name_b, date, phone_a, phone_b, other) \
      FROM '/Users/blauerbock/workspaces/dlt-pipeline/flatten-jsonb/contacts.csv' \
      WITH (format csv, header true);"

CREATE OR REPLACE VIEW public.contacts_flat AS
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
FROM public.contacts;


-- generate SQL with key names as columns:s

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

--result of above:
SELECT name_a,
       name_b,
       date,
        phone_a,
        phone_b,
other->>'vip' AS vip,
other->>'age' AS age,
other->>'city' AS city,
other->>'preferred_contact' AS preferred_contact
FROM contacts;
