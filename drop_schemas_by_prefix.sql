DO
$$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT nspname
        FROM   pg_namespace
        WHERE  nspname LIKE 'bronze_2%'
    LOOP
        EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE;', r.nspname);
    END LOOP;
END;
$$;
