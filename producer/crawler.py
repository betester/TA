import asyncio
from crawler import Crawler


class CrawlerProducer:
    
    def __init__(self, crawler: Crawler):
        self.crawler = crawler
        self._queue = asyncio.Queue(100)
        self.stop = False
    
    async def start_crawler(self):
        while not self.stop:
            uuid = await self.crawler.crawl()
            await self._queue.put(uuid)
    
    def stop_crawl(self):
        self.stop = True
    
    @property
    def queue(self):
        return self._queue
