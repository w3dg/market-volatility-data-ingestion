import os
from typing import Optional

import requests as r

from utils.file_utils import load_json, save_json


def fetchNewsDataIO() -> Optional[list[dict]]:
    NEWSDATA_API_KEY = os.getenv("NEWSDATA_API_KEY")
    if NEWSDATA_API_KEY is None:
        print("NewsData API Key Not found in environment")
        return None
    NEWSDATA_URL = f"https://newsdata.io/api/1/crypto?apikey={NEWSDATA_API_KEY}"
    res = r.get(NEWSDATA_URL)
    if res.status_code != 200:
        print("Could not fetch NewsData")
        return None
    jsonresponse = res.json()
    jsonresults = jsonresponse.get("results", [])
    extracted_entries = []

    for entry in jsonresults:
        extracted_entry = {
            "id": entry.get("article_id"),
            "title": entry.get("title"),
            "link": entry.get("link"),
            "description": entry.get("description"),
            "pubDate": entry.get("pubDate"),
            "source_id": entry.get("source_id"),
            "source_name": entry.get("source_name"),
        }
        extracted_entries.append(extracted_entry)

    save_json("newsdata.json", extracted_entries)
    return extracted_entries


def getNewsData():
    news: Optional[list[dict]] = None
    if os.getenv("USE_CACHE") == 1:
        news = load_json("newsdata.json")
    else:
        news = fetchNewsDataIO()

    if news is not None:
        print("newsdata.io fetched")
        print("Total articles fetched:", len(news))
