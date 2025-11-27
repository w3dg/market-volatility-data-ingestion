from dotenv import load_dotenv

import gcloud.bq as bq
from extractors.coindesk import getCoinDesk
from extractors.cointelegraph import getCoinTelegraph
from extractors.cryptopanic import getCryptoPanicData
from extractors.newsdataio import getNewsData
from extractors.reddit import getRedditData
from extractors.yfinance import getYFinanceData
from utils.file_utils import convertToBQJSONFormat

load_dotenv()


def main():
    print("Starting data extraction...\n")
    print("Fetching CoinTelegraph data:")
    getCoinTelegraph()
    print("\nFetching CoinDesk data:")
    getCoinDesk()
    print("\nFetching NewsData.io data:")
    getNewsData()
    print("\nFetching CryptoPanic data:")
    getCryptoPanicData()
    print("\nFetching YFinance data:")
    getYFinanceData()
    print("\nFetching Reddit data:")
    getRedditData()

    filenames = [
        "coindesk",
        "cointelegraph",
        "cryptopanic",
        "newsdata",
        "reddit_data",
        "yfinance_news",
        "yfinance_tickers",
    ]

    for filename in filenames:
        infile = f"{filename}.json"
        outfile = f"{filename}_bq.json"
        convertToBQJSONFormat(infile, outfile)
        print("converted ", infile, " to flat json format in ", outfile)

    bq.ingestCoindesk()
    bq.ingestCointelegraph()
    bq.ingestCryptopanic()
    bq.ingestNewsdata()
    bq.ingestReddit()
    bq.ingestYFinanceNews()
    bq.ingestYFinanceTickers()


if __name__ == "__main__":
    main()
