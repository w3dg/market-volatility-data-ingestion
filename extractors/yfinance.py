import os

import yfinance as yf

from utils.file_utils import load_json, save_json


def fetchYFinance() -> tuple[list, list]:
    # List of crypto tickers to fetch data for
    crypto_tickers = ["BTC-USD", "ETH-USD", "ADA-USD", "SOL-USD", "DOT-USD"]

    # Uncomment the following lines to fetch from API
    tickerdata = []
    extracted_entries = []

    for ticker in crypto_tickers:
        try:
            ticker_obj = yf.Ticker(ticker)
            news = ticker_obj.news
            info = ticker_obj.info

            for entry in news:
                e = entry.get("content")
                extracted_entry = {
                    "id": e.get("id"),
                    "title": e.get("title"),
                    "description": f"{e.get('summary', '')}. {e.get('description', '')}",
                    "link": e.get("previewUrl"),
                    "publisher": e.get("publisher"),
                    "published_date": e.get("pubDate"),
                    "ticker": ticker,
                }
                extracted_entries.append(extracted_entry)
            tickerdata.append(
                {
                    "ticker": ticker,
                    "regularMarketChange": info.get("regularMarketChange"),
                    "exchange": info.get("exchange"),
                    "dayLow": info.get("dayLow"),
                    "dayHigh": info.get("dayHigh"),
                    "open": info.get("open"),
                    "currency": info.get("currency"),
                    "priceHint": info.get("priceHint"),
                    "regularMarketPreviousClose": info.get(
                        "regularMarketPreviousClose"
                    ),
                    "regularMarketOpen": info.get("regularMarketOpen"),
                    "regularMarketDayLow": info.get("regularMarketDayLow"),
                    "regularMarketDayHigh": info.get("regularMarketDayHigh"),
                    "regularMarketChangePercent": info.get(
                        "regularMarketChangePercent"
                    ),
                    "regularMarketChangePrice": info.get("regularMarketChangePrice"),
                }
            )
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

    # Save to file
    save_json("yfinance_tickers.json", tickerdata)
    save_json("yfinance_news.json", extracted_entries)

    return extracted_entries, tickerdata


def getYFinanceData():
    if os.getenv("USE_CACHE") == 1:
        extracted_entries = load_json("yfinance_news.json")
        if extracted_entries is None:
            extracted_entries = []
        tickerdata = load_json("yfinance_tickers.json")
        if tickerdata is None:
            tickerdata = []
    else:
        extracted_entries, tickerdata = fetchYFinance()

    extracted_entries, tickerdata = fetchYFinance()
    print(f"Fetched {len(extracted_entries)} news entries from YFinance.")
    print(f"Fetched {len(tickerdata)} ticker info from YFinance.")
