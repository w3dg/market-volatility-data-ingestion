import os
from typing import Optional

import requests as r
from fastfeedparser import FastFeedParserDict
from fastfeedparser import parse as fastfeedparse

from utils.file_utils import load_json, save_json


def fetchCoinTelegraphNews() -> Optional[FastFeedParserDict]:
    COIN_TELEGRAPH_URL = "https://cointelegraph.com/rss"
    res = r.get(COIN_TELEGRAPH_URL)
    if res.status_code != 200:
        print("Could not fetch Coin telegraph")
        return None
    myfeed = fastfeedparse(res.text)
    feedentries = myfeed.get("entries", [])
    extracted_entries = []
    for entry in feedentries:
        extracted_entry = {
            "id": entry.get("id"),
            "title": entry.get("title"),
            "link": entry.get("link"),
            "published": entry.get("published"),
            "description": entry.get("description"),
        }
        extracted_entries.append(extracted_entry)
    save_json("cointelegraph.json", extracted_entries)

    return myfeed


def getCoinTelegraph():
    ctnews = None

    if os.getenv("USE_CACHE") == 1:
        ctnews: Optional[FastFeedParserDict] = load_json("cointelegraph.json")
    else:
        ctnews = fetchCoinTelegraphNews()

    if ctnews is not None:
        news_entries = ctnews["entries"]
        print(f"{len(news_entries)} items from coin telegraph fetched")
