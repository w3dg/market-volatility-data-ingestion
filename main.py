from dotenv import load_dotenv
from extractors.cointelegraph import getCoinTelegraph
from extractors.coindesk import getCoinDesk
from extractors.newsdataio import getNewsData
from extractors.cryptopanic import getCryptoPanicData
from extractors.reddit import getRedditData
from extractors.yfinance import getYFinanceData

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


if __name__ == "__main__":
    main()
