"""BigQuery 공통 유틸리티"""
import os
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "steam-service-492701")
DATASET = os.getenv("BQ_DATASET", "steam_data")

_client = None


def get_bq_client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(project=PROJECT_ID)
    return _client


def insert_rows(client: bigquery.Client, table_name: str, rows: list,
                batch_size: int = 500, raise_on_error: bool = False):
    if not rows:
        print(f"  SKIP: {table_name} (0건)")
        return
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            print(f"  ERROR ({table_name}): {errors[:2]}")
            if raise_on_error:
                raise RuntimeError(f"Failed to insert into {table_name}")
        else:
            print(f"  OK: {len(batch)}rows -> {table_name}")


def get_all_app_ids(client: bigquery.Client) -> set:
    query = f"SELECT DISTINCT app_id FROM `{PROJECT_ID}.{DATASET}.games`"
    result = client.query(query).result()
    return {row.app_id for row in result}
