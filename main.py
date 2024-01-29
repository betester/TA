
import asyncio
from consumer.crawler import CrawlerConsumer

from crawler.mock_up import MockUpCrawler
from producer.crawler import CrawlerProducer


async def main():
        
    loop = asyncio.get_running_loop()

    crawler = MockUpCrawler(1, 2)
    crawler_producer = CrawlerProducer(crawler)
    
    try:
        loop.create_task(crawler_producer.start_crawler())
        queue = crawler_producer.queue

        c1, c2 = CrawlerConsumer(1, queue), CrawlerConsumer(2, queue)
        await asyncio.gather(c1.consume(), c2.consume())

    except KeyboardInterrupt:
        crawler_producer.stop_crawl()



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
