from typing import Optional
from crawler import Crawler, CrawlerResponse
from fogverse import Producer, Profiling
from fogverse.fogverse_logging import get_logger

class CrawlerProducer(Producer, Profiling):

    def __init__(self, producer_topic: str, producer_servers: list[str] | str, crawler: Crawler):
        self.producer_topic = producer_topic 
        self.producer_servers = producer_servers
        self._crawler = crawler
        self.__log = get_logger(name=self.__class__.__name__)
        Producer.__init__(self)
        Profiling.__init__(self, name='crawler-logs', dirname='crawler-logs')
        self._closed = False

    async def receive(self):
        try:
            data: Optional[CrawlerResponse] = await self._crawler.crawl()
            if data: 
                return data.message

        except Exception:
            self.__log.error("No more file to read")
            # stops the crawler
            self._closed = True

