import os
from clickhouse_driver import Client
import pandas as pd

# ───────────────────────────────────────────────
#  ClickHouse connection settings
#  Override via environment variables in docker-compose
# ───────────────────────────────────────────────
CH_HOST     = os.getenv("CLICKHOUSE_HOST",     "clickhouse")
CH_USER     = os.getenv("CLICKHOUSE_USER",      "clickhouseadmin")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD",  "clickhouseadmin")

REFRESH_INTERVAL_MS = int(os.getenv("REFRESH_INTERVAL_MS", "10000"))


def get_client() -> Client:
    return Client(
        host=CH_HOST,
        user=CH_USER,
        password=CH_PASSWORD,
    )


def query_df(sql: str, params: dict = None) -> pd.DataFrame:
    """Execute a ClickHouse query and return a pandas DataFrame."""
    client = get_client()
    try:
        result, columns = client.execute(sql, params or {}, with_column_types=True)
        col_names = [c[0] for c in columns]
        return pd.DataFrame(result, columns=col_names)
    except Exception as e:
        print(f"[ClickHouse Error] {e}")
        return pd.DataFrame()
    finally:
        client.disconnect()
