import os
from concurrent.futures import TimeoutError

from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery

import gcloud.schemas as schemas

load_dotenv()

client = bigquery.Client(os.getenv("GCP_BQ_PROJECT_NAME", "market-volatility"))
dataset_name = os.getenv("GCP_BQ_DATASET_NAME", "sources")
dataset_ref = client.dataset(dataset_name)


def createTableIfNotExists(table_ref, schema):
    table = bigquery.Table(table_ref, schema)
    try:
        table = client.create_table(table)
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
    except Exception as e:
        print(
            f"Table {table.project}.{table.dataset_id}.{table.table_id} already exists, not creating again."
        )


def execute_load_job(inputfilename, table_ref, job_config):
    inputfile = f"bq_data/{inputfilename}"
    with open(inputfile, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    try:
        job.result()  # Waits for the job to complete.
        print(f"Loaded {job.output_rows} rows into {table_ref.path}.")
    except TimeoutError as e:
        print(f"Ingest Job for loading {inputfile} timed out:", e)
    except GoogleAPICallError as e:
        print(f"Ingest API call for loading {inputfile} failed:", e)


def ingestCoindesk():
    table_ref = dataset_ref.table("coindesk")
    createTableIfNotExists(table_ref, schema=schemas.coindesk_schema)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schemas.coindesk_schema,
    )
    execute_load_job("coindesk_bq.json", table_ref, job_config)


def ingestCointelegraph():
    table_ref = dataset_ref.table("cointelegraph")
    createTableIfNotExists(table_ref, schema=schemas.cointelegraph_schema)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schemas.cointelegraph_schema,
    )
    execute_load_job("cointelegraph_bq.json", table_ref, job_config)


def ingestCryptopanic():
    table_ref = dataset_ref.table("cryptopanic")
    createTableIfNotExists(table_ref, schema=schemas.cryptopanic_schema)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schemas.cryptopanic_schema,
    )
    execute_load_job("cryptopanic_bq.json", table_ref, job_config)


def ingestNewsdata():
    table_ref = dataset_ref.table("newsdata")
    createTableIfNotExists(table_ref, schema=schemas.newsdata_schema)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schemas.newsdata_schema,
    )
    execute_load_job("newsdata_bq.json", table_ref, job_config)


def ingestReddit():
    table_ref = dataset_ref.table("reddit")
    createTableIfNotExists(table_ref, schema=schemas.reddit_schema)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schemas.reddit_schema,
    )
    execute_load_job("reddit_data_bq.json", table_ref, job_config)


def ingestYFinanceNews():
    table_ref = dataset_ref.table("yfinance_news")
    createTableIfNotExists(table_ref, schema=schemas.yfinance_news_schema)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schemas.yfinance_news_schema,
    )
    execute_load_job("yfinance_news_bq.json", table_ref, job_config)


def ingestYFinanceTickers():
    table_ref = dataset_ref.table("yfinance_tickers")
    createTableIfNotExists(table_ref, schema=schemas.yfinance_tickers_schema)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schemas.yfinance_tickers_schema,
    )
    execute_load_job("yfinance_tickers_bq.json", table_ref, job_config)


def listAllTables():
    tables = list(client.list_tables(dataset_ref))
    print(f"Tables in dataset {len(tables)}")
    for table in tables:
        print(f"Table: {table.table_id}")
