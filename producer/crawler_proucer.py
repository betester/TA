from crawler import Crawler
from fogverse import Producer 
from fogverse.fogverse_logging import get_logger

class CrawlerProducer(Producer):

    def __init__(self, producer_topic: str, producer_servers: list[str] | str, crawler: Crawler):
        self.producer_topic = producer_topic 
        self.producer_servers = producer_servers
        self._crawler = crawler
        self.__log = get_logger(name=self.__class__.__name__)
        Producer.__init__(self)

    async def receive(self):
        try:
            return self._crawler.crawl()
        except Exception as e:
            self.__log.error(e)
