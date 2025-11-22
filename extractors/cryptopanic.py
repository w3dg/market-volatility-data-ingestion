import os
from typing import Optional
import json
import requests as r
from utils.file_utils import save_json, load_json


def fetchCryptoPanic() -> Optional[list[dict]]:
    CRYPTOPANIC_API_KEY = os.getenv("CRYPTOPANIC_API_KEY")
    if CRYPTOPANIC_API_KEY is None:
        print("CryptoPanic API Key Not found in environment")
        return None
    CRYPTOPANIC_URL = (
        f"https://cryptopanic.com/api/v1/posts/?auth_token={CRYPTOPANIC_API_KEY}"
    )

    # Uncomment the following lines to fetch from API
    extracted_entries = []
    res = r.get(CRYPTOPANIC_URL)
    if res.status_code != 200:
        print("Could not fetch CryptoPanic")
        return None
    jsonresponse = res.json()

    results = jsonresponse.get("results", [])
    for entry in results:
        extracted_entry = {
            "id": entry.get("id"),
            "title": entry.get("title"),
            "description": entry.get("description"),
            "published_at": entry.get("published_at"),
        }
        extracted_entry["url"] = (
            f"https://cryptopanic.com/{entry.get("kind")}/{entry.get('id')}/{entry.get('slug')}"
        )

        extracted_entries.append(extracted_entry)

    # Save to file
    save_json("cryptopanic.json", extracted_entries)
    return extracted_entries

    # Load from saved file instead
    # extracted_entries = load_json("cryptopanic.json")
    # return extracted_entries


def getCryptoPanicData():
    newsjson = fetchCryptoPanic()
    if newsjson is not None:
        print("cryptopanic data fetched")
        for post in newsjson:
            print(post["title"])
            print(post["description"])
            print(post["url"])
            print(post["published_at"])
            print("-----")
