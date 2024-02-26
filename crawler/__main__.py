import asyncio

from .component import CrawlerComponent
from master.component import MasterComponent

async def main():
    analyzer_component = CrawlerComponent()
    master_component = MasterComponent()

    analyzer_event_handler = analyzer_component.mock_disaster_crawler(
        directory_path="./data/crawler/kaggle",
        observer= master_component.producer_observer()
    )
    return await analyzer_event_handler.run()

if __name__ == "__main__":
    asyncio.run(main())
