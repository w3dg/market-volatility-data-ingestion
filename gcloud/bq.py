import os
from concurrent.futures import TimeoutError

import pandas as pd
from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery

from gcloud.tableconfig import getTableConfig

load_dotenv()

PROJECT_NAME = os.getenv("GCP_BQ_PROJECT_NAME", "market-volatility")
DATASET_NAME = os.getenv("GCP_BQ_DATASET_NAME", "sources")

client = bigquery.Client(PROJECT_NAME)
dataset_ref = client.dataset(DATASET_NAME)

TABLE_CONFIG = getTableConfig()


def createTableIfNotExists(table_ref, schema):
    table = bigquery.Table(table_ref, schema)
    try:
        table = client.create_table(table)
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
    except Exception:
        print(
            f"Table {table.project}.{table.dataset_id}.{table.table_id} already exists, not creating again."
        )


def build_merge_query(
    base_table: str,
    staging_table: str,
    key_columns: list[str],
    all_columns: list[str],
) -> str:
    full_base_table_name = f"`{PROJECT_NAME}.{DATASET_NAME}.{base_table}`"
    full_staging_table_name = f"`{PROJECT_NAME}.{DATASET_NAME}.{staging_table}`"

    # ON clause: T.key1 = S.key1 AND T.key2 = S.key2 ...
    on_clause = " AND ".join([f"T.{col} = S.{col}" for col in key_columns])

    # UPDATE SET T.col = S.col for all non-key columns
    non_key_cols = [c for c in all_columns if c not in key_columns]
    update_assignments = ",\n        ".join([f"T.{c} = S.{c}" for c in non_key_cols])

    # INSERT (col1, col2, ...) VALUES (S.col1, S.col2, ...)
    insert_cols = ", ".join(all_columns)
    insert_values = ", ".join([f"S.{c}" for c in all_columns])

    merge_query = f"""
    MERGE {full_base_table_name} T
    USING {full_staging_table_name} S
    ON {on_clause}
    WHEN MATCHED THEN
      UPDATE SET
        {update_assignments}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols})
      VALUES ({insert_values});
    """
    return merge_query


def dedupe_df(df, key_cols, order_cols=None):
    """
    Keep one row per key, optionally preferring the 'latest' based on order_cols.
    To avoid having the following error, `UPDATE/MERGE must match at most one source row for each target row`
    """
    if order_cols:
        df = df.sort_values(order_cols)
    return df.drop_duplicates(
        subset=key_cols, keep="last"
    )  # keeps the last row in the sorted order


def upsert_into_bq(table_name: str):
    cfg = TABLE_CONFIG[table_name]

    all_columns = [field.name for field in cfg["schema"]]
    query = build_merge_query(
        base_table=cfg["table"],
        staging_table=cfg["staging_table"],
        key_columns=cfg["keys"],
        all_columns=all_columns,
    )

    print(f"Merging into BQ table {table_name}")
    # print(query)

    job = client.query(query)
    job.result()
    print(f"[{table_name}] MERGE completed.")


def execute_load_job(
    df: pd.DataFrame,
    table_ref: bigquery.TableReference,
    schema,
):
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    try:
        job.result()  # Wait for the job to complete
        print(f"Loaded {job.output_rows} rows into {table_ref.path}.")
    except TimeoutError as e:
        print("Ingest Job for loading DataFrame timed out:", e)
    except GoogleAPICallError as e:
        print("Ingest API call for loading DataFrame failed:", e)


def execute_load_job_staging(
    df: pd.DataFrame,
    table_ref: bigquery.TableReference,
    schema,
):
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    try:
        job.result()  # Wait for the job to complete
        print(f"Loaded {job.output_rows} rows into staging table {table_ref.path}.")
    except TimeoutError as e:
        print("Ingest Job for loading DataFrame timed out:", e)
    except GoogleAPICallError as e:
        print("Ingest API call for loading DataFrame failed:", e)


def ingestTable(df: pd.DataFrame, table_name: str):
    if table_name not in TABLE_CONFIG:
        raise Exception(
            "Unknown table name provided for ingestion. Table details not known."
        )

    table = TABLE_CONFIG[table_name]

    df = dedupe_df(df, key_cols=table["keys"], order_cols=table["order_cols"])

    staging_table_ref = dataset_ref.table(table["staging_table"])
    createTableIfNotExists(staging_table_ref, schema=table["schema"])
    execute_load_job_staging(df, staging_table_ref, table["schema"])

    table_ref = dataset_ref.table(table["table"])
    createTableIfNotExists(table_ref, schema=table["schema"])
    upsert_into_bq(table_name)


def ingestCoindesk(df: pd.DataFrame):
    ingestTable(df, "coindesk")


def ingestCointelegraph(df: pd.DataFrame):
    ingestTable(df, "cointelegraph")


def ingestCryptopanic(df: pd.DataFrame):
    ingestTable(df, "cryptopanic")


def ingestNewsdata(df: pd.DataFrame):
    ingestTable(df, "newsdata")


def ingestReddit(df: pd.DataFrame):
    ingestTable(df, "reddit")


def ingestYFinanceNews(df: pd.DataFrame):
    ingestTable(df, "yfinance_news")


def ingestYFinanceTickers(df: pd.DataFrame):
    ingestTable(df, "yfinance_tickers")


def listAllTables():
    tables = list(client.list_tables(dataset_ref))
    print(f"Tables in dataset {len(tables)}")
    for table in tables:
        print(f"Table: {table.table_id}")
