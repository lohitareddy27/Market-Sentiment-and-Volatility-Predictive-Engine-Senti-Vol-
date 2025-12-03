from dotenv import load_dotenv
load_dotenv()

from fred_ingest import main as fred_main
from market_ingest import main as market_main
from youtube_ingest import main as youtube_main
from reddit_ingest import main as reddit_main
from news_ingest import main as news_main
from yahoonews_ingest import main as yahoonews_main


def main():

    print("FRED ingestion")
    fred_main()

    print("\nMarket ingestion")
    market_main()

    print("\nYouTube ingestion")
    youtube_main()

    print("\nReddit ingestion")
    reddit_main()

    print("\nNewsAPI ingestion")
    news_main()

    print("\nYahoo News ingestion")
    yahoonews_main()


if __name__ == "__main__":
    main()