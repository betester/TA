import asyncio

from .component import CrawlerComponent

async def main():
    analyzer_component = CrawlerComponent()
    analyzer_event_handler = analyzer_component.mock_disaster_crawler(
        directory_path="./data/crawler/kaggle"
    )
    return await analyzer_event_handler.run()

if __name__ == "__main__":
    asyncio.run(main())
