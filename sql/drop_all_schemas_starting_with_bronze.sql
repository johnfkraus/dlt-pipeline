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
