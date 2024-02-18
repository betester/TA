import asyncio

from typing import Optional

from .crawler import Crawler, CrawlerResponse
from fogverse import Producer, Profiling
from fogverse.fogverse_logging import get_logger

class CrawlerProducer(Producer):

    def __init__(self, 
                 producer_topic: str, 
                 producer_servers: list[str] | str, 
                 consumer_group_id: str,
                 crawler: Crawler):
        # kafka args
        self.producer_topic = producer_topic 
        self.producer_servers = producer_servers
        self._crawler = crawler
        self.group_id = consumer_group_id

        self._queue = asyncio.Queue()
        
        self.__log = get_logger(name=self.__class__.__name__)
        Producer.__init__(self)
        self.auto_decode = False
        self._closed = False

    async def receive(self):
        try:
            data: Optional[CrawlerResponse] = await self._crawler.crawl()
            if data: 
                return data

        except Exception:
            self.__log.error("No more file to read")
            # stops the crawler
            self._closed = True

    def encode(self, data: CrawlerResponse) -> bytes:
        return data.model_dump_json().encode()
