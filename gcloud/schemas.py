from google.cloud import bigquery

coindesk_schema = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("link", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("published", "STRING", mode="REQUIRED"),
]

cointelegraph_schema = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("link", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("published", "STRING", mode="REQUIRED"),
]

cryptopanic_schema = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("published_at", "STRING", mode="REQUIRED"),
]

newsdata_schema = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("link", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("pubDate", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_name", "STRING", mode="REQUIRED"),
]

reddit_schema = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("created_utc", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("subreddit", "STRING", mode="REQUIRED"),
]

yfinance_news_schema = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("link", "STRING", mode="NULLABLE"),  # link can be null
    bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("published_date", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
]

yfinance_tickers_schema = [
    bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("regularMarketChange", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("exchange", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("dayLow", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("dayHigh", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("open", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("priceHint", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("regularMarketPreviousClose", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("regularMarketOpen", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("regularMarketDayLow", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("regularMarketDayHigh", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("regularMarketChangePercent", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("regularMarketChangePrice", "FLOAT64", mode="REQUIRED"),
]
