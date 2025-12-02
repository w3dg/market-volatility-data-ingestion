import gcloud.schemas as schemas


def getTableConfig():
    return TABLE_CONFIG


TABLE_CONFIG = {
    "coindesk": {
        "table": "coindesk",
        "staging_table": "coindesk_staging",
        "schema": schemas.coindesk_schema,
        "keys": ["id"],
        "order_cols": ["published"],
    },
    "cointelegraph": {
        "table": "cointelegraph",
        "staging_table": "cointelegraph_staging",
        "schema": schemas.cointelegraph_schema,
        "keys": ["id"],
        "order_cols": ["published"],
    },
    "cryptopanic": {
        "table": "cryptopanic",
        "staging_table": "cryptopanic_staging",
        "schema": schemas.cryptopanic_schema,
        "keys": ["id"],
        "order_cols": ["published_at"],
    },
    "newsdata": {
        "table": "newsdata",
        "staging_table": "newsdata_staging",
        "schema": schemas.newsdata_schema,
        "keys": ["id"],
        "order_cols": ["pubDate"],
    },
    "reddit": {
        "table": "reddit",
        "staging_table": "reddit_staging",
        "schema": schemas.reddit_schema,
        "keys": ["id"],
        "order_cols": ["created_utc"],
    },
    "yfinance_news": {
        "table": "yfinance_news",
        "staging_table": "yfinance_news_staging",
        "schema": schemas.yfinance_news_schema,
        "keys": ["id"],
        "order_cols": ["ticker", "published_date"],
    },
    "yfinance_tickers": {
        "table": "yfinance_tickers",
        "staging_table": "yfinance_tickers_staging",
        "schema": schemas.yfinance_tickers_schema,
        "keys": ["date", "ticker"],
        "order_cols": ["date", "ticker"],
    },
}
