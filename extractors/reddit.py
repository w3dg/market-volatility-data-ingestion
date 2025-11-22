import requests as r
from concurrent.futures import ThreadPoolExecutor

from utils.file_utils import save_json, load_json

subreddits = ["CryptoCurrency", "ethfinance", "CryptoMarkets", "ethereum"]
# r/ethereum has daily discussion posts


def fetchSubreddit(subreddit, limit=10):
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={limit}"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = r.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Failed to fetch data from r/{subreddit}: {response.status_code}")
        return []

    data = response.json()
    posts = data.get("data", {}).get("children", [])
    return posts


def fetchRedditPosts(subreddits, limit=10):
    extracted_posts = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        list_all_posts = (
            list(executor.map(fetchSubreddit, subreddits, len(subreddits) * [limit])),
        )[0]
        for posts in list_all_posts:
            extracted_posts.extend(posts)
    results = []
    for post in extracted_posts:
        post_data = post["data"]
        results.append(
            {
                "id": f"{post_data.get("subreddit_id")}-{post_data.get("id")}",
                "title": post_data.get("title"),
                "url": post_data.get("url"),
                "description": post_data.get("selftext"),
                "created_utc": post_data.get("created_utc"),
                "subreddit": post_data.get("subreddit"),
            }
        )
    save_json("reddit_data.json", results)
    return results

    # load from saved file
    # results = load_json("reddit_data.json")
    # return results


def getRedditData():
    all_posts = fetchRedditPosts(subreddits, limit=10)
    return all_posts
