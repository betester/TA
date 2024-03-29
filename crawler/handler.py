
from aiokafka.client import asyncio
from master.master import ProducerObserver

from .crawler import Crawler, CrawlerResponse
from fogverse import Producer, Profiling
from fogverse.fogverse_logging import get_logger

class CrawlerProducer(Producer, Profiling):

    def __init__(self, 
                 producer_topic: str, 
                 producer_servers: list[str] | str, 
                 consumer_group_id: str,
                 crawler: Crawler,
                 observer: ProducerObserver):
        # kafka args
        self.producer_topic = producer_topic 
        self.producer_servers = producer_servers
        self._crawler = crawler
        self.group_id = consumer_group_id
        
        Producer.__init__(self)
        Profiling.__init__(self, name='crawler-logs', dirname='crawler-logs')

        self.auto_decode = False
        self._closed = False

        self.__log = get_logger(name=self.__class__.__name__)
        self._observer = observer


    async def receive(self):
        try:
            # data: Optional[CrawlerResponse] = await self._crawler.crawl()
            # if data: 
            #     return data
            await asyncio.sleep(0.01)
            return CrawlerResponse(
                message="H" * 255,
                source="Me"
            )

        except Exception:
            self.__log.error("No more file to read")
            # stops the crawler
            self._closed = True

    def encode(self, data: CrawlerResponse) -> bytes:
        return data.model_dump_json().encode()

    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        result = await super().send(data, topic, key, headers, callback)
        await self._observer.send_total_successful_messages(
            self.producer_topic,
            1,
            lambda x, y: self.producer.send(topic=x, value=y)
        )
        return result


