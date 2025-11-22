from typing import Optional
import requests as r
from fastfeedparser import FastFeedParserDict, parse as fastfeedparse

from utils.file_utils import save_json, load_json


def fetchCoinDeskNews() -> Optional[FastFeedParserDict]:
    COIN_DESK_URL = "https://www.coindesk.com/arc/outboundfeeds/rss"
    res = r.get(COIN_DESK_URL)
    if res.status_code != 200:
        print("Could not fetch CoinDesk")
        return None
    myfeed = fastfeedparse(res.text)
    feedentries = myfeed.get("entries", [])
    extracted_entries = []
    for entry in feedentries:
        extracted_entry = {
            "id": entry.get("id"),
            "title": entry.get("title"),
            "link": entry.get("link"),
            "description": entry.get("description"),
            "published": entry.get("published"),
        }
        extracted_entries.append(extracted_entry)
    save_json("coindesk.json", extracted_entries)

    # Load from saved file
    # myfeed = load_json("coindesk.json")

    return myfeed


def getCoinDesk():
    ctnews = fetchCoinDeskNews()
    if ctnews is not None:
        news_entries = ctnews["entries"]
        print(f"{len(news_entries)} items from coindesk fetched")
