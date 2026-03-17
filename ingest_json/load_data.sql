CREATE TABLE public.contacts (
    name_a  text,
    name_b  text,
    date    date,
    phone_a text,
    phone_b text,
    other   jsonb
);

from mac terminal command line:

psql -h localhost -p 5432 -U postgres -d comms \
  -c "\copy public.contacts (name_a, name_b, date, phone_a, phone_b, other) \
      FROM '/Users/blauerbock/workspaces/dlt-pipeline/flatten-jsonb/contacts.csv' \
      WITH (format csv, header true);"




SELECT DISTINCT jsonb_object_keys(other) AS key
FROM contacts
ORDER BY key;
