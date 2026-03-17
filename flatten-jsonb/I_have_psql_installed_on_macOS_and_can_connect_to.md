<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# I have psql installed on macOS and can connect to localhost

Great, then you can do everything with a single command from your Mac terminal using `\copy`.

## One-shot import command

Run this in your macOS terminal:

```bash
psql -h localhost -p 5432 -U your_user -d your_db \
  -c "\copy public.contacts (name_a, name_b, date, phone_a, phone_b, other) \
      FROM '/Users/blauerbock/workspaces/dlt-pipeline/flatten-jsonb/contacts.csv' \
      WITH (format csv, header true);"
```

Replace:

- `your_user` with your DB username
- `your_db` with your database name

Notes:

- `\copy` runs on your Mac and reads the file from the macOS path you provided, then streams it into Postgres in the container.[^1][^2]
- `FORMAT csv, HEADER true` assumes the first row of the CSV has column names.[^3][^4]

If your password is prompted, just enter it when psql asks.

Do you want a tiny bash script that wraps this so you can re-run imports without retyping the command?

<div align="center">⁂</div>

[^1]: https://stackoverflow.com/questions/73478900/how-to-copy-a-csv-into-a-postgresql-docker-container

[^2]: https://stackoverflow.com/questions/66934372/importing-csv-files-from-local-machine-to-postgresql-docker-container-using-pgad

[^3]: https://neon.com/postgresql/postgresql-tutorial/import-csv-file-into-posgresql-table

[^4]: https://wiki.postgresql.org/wiki/COPY

