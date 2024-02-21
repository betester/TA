from aiokafka.client import asyncio
from analyzer.component import AnalyzerComponent
from crawler.component import CrawlerComponent

async def main():

    # loop
    loop = asyncio.get_event_loop()
    
    # components 
    analyzer_component = AnalyzerComponent()
    crawler_component = CrawlerComponent()
    
    # producers or consumers
    kaggle_crawler_producer = crawler_component.mock_disaster_crawler(
        directory_path="./data/crawler/kaggle"
    )
    analyzer_producer = analyzer_component.disaster_analyzer()

    analyzer_task = loop.create_task(analyzer_producer.run()) 
    kaggle_crawler_task = loop.create_task(kaggle_crawler_producer.run())

    return await asyncio.gather(
        analyzer_task,
        kaggle_crawler_task,
    )


if __name__ == "__main__":
    asyncio.run(main())
