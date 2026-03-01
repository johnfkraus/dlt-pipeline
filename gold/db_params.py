# gold/db_params.py
import tomllib
import polars as pl

def load_db_params_from_secrets(
        path: str = ".dlt/secrets.toml",
        section: str = "destination.postgres.credentials",
) -> dict:
    """
    Read Postgres connection parameters from .dlt/secrets.toml.
    Expects either discrete fields (database, username, password, host, port)
    or a DSN string.
    """
    secrets_path = Path(path)
    if not secrets_path.exists():
        raise FileNotFoundError(f"Secrets file not found at {path}")

    with secrets_path.open("rb") as f:
        cfg = tomllib.load(f)

    # Nested keys are represented as nested tables in TOML
    dest_cfg = cfg
    for part in section.split("."):
        dest_cfg = dest_cfg.get(part, {})

    if not dest_cfg:
        raise KeyError(f"Section [{section}] not found in {path}")

    # DSN style (optional)
    dsn = dest_cfg.get("dsn")
    if dsn:
        return {"dsn": dsn}

    # Discrete fields style
    return {
        "dbname": dest_cfg.get("database"),
        "user": dest_cfg.get("username"),
        "password": dest_cfg.get("password"),
        "host": dest_cfg.get("host", "localhost"),
        "port": dest_cfg.get("port", 5432),
    }


def set_polars_col_width_display(num_characters = 200):
    pl.Config.set_fmt_str_lengths(num_characters)

