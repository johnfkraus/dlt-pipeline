# db_config.py
import os
from dotenv import load_dotenv  # pip install python-dotenv

load_dotenv()  # loads .env from project root

def get_pg_conn_params():
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", "your_database_name"),
        "user": os.getenv("PGUSER", "your_app_user"),
        "password": os.getenv("PGPASSWORD", "your_app_password"),
    }

