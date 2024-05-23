
from aiokafka.client import asyncio
from master.master import ProducerObserver

from .crawler import Crawler, CrawlerResponse
from fogverse import Producer, Profiling
from fogverse.fogverse_logging import get_logger
import time

class CrawlerProducer(Producer, Profiling):

    def __init__(self, 
                 producer_topic: str, 
                 producer_servers: list[str] | str, 
                 consumer_group_id: str,
                 crawler: Crawler,
                 observer: ProducerObserver,
                 metadata_max_age_ms: int,
                 crawler_delay : float,
                 spam_delay : float= 600, 
                 spam_duration: float= 60,
                 spam_rate : float = 0.01,
                 use_spam: int = 0
                 ):
        # kafka args
        self.producer_topic = producer_topic 
        self.producer_servers = producer_servers
        self._crawler = crawler
        self.group_id = consumer_group_id
        self.crawler_delay = crawler_delay

        self.spam_delay =  spam_delay
        self.spam_duration = spam_duration
        self.spam_rate = spam_rate

        self.producer_conf = {
            'metadata_max_age_ms' : metadata_max_age_ms
        } 
        
        Producer.__init__(self)
        Profiling.__init__(self, name='crawler-logs', dirname='crawler-logs')

        self.auto_decode = False
        self._closed = False

        self.__log = get_logger(name=self.__class__.__name__)
        self._observer = observer

        if use_spam:
            asyncio.create_task(self.spam())

    async def spam(self):
        self.__log.info("Initiating spam")
        await asyncio.sleep(self.spam_delay)
        self.__log.info("Starting spam")
        old_value = self.crawler_delay 
        self.crawler_delay = self.spam_rate
        await asyncio.sleep(self.spam_duration)
        self.__log.info("Terminating spam")
        self.crawler_delay = old_value

    async def receive(self):
        try:
            # data: Optional[CrawlerResponse] = await self._crawler.crawl()
            # if data: 
            #     return data
            await asyncio.sleep(self.crawler_delay)
            return CrawlerResponse(
                message="H" * 10,
                source="Me",
                timestamp=time.time()
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


