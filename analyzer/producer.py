
from analyzer import DisasterAnalyzer
from analyzer.contract import DisasterAnalyzerResponse
from crawler.contract import CrawlerResponse
from fogverse import Producer, Consumer, Profiling
from fogverse.fogverse_logging import get_logger

class AnalyzerProducer(Consumer, Producer, Profiling):

    def __init__(self, 
                 producer_topic: str, 
                 producer_servers: list[str] | str, 
                 consumer_topic: str, 
                 consumer_servers: str,
                 consumer_group_id: str,
                 analyzer: DisasterAnalyzer):

        self.consumer_topic =  consumer_topic
        self.consumer_servers = consumer_servers
        self.producer_topic = producer_topic 
        self.producer_servers = producer_servers
        self.consumer_group = consumer_group_id
        self._analyzer = analyzer
        self.__log = get_logger(name=self.__class__.__name__)
        self.auto_decode = False

        Producer.__init__(self)
        Consumer.__init__(self)
        Profiling.__init__(self, name='analyzer-logs', dirname='analyzer-logs')
        self._closed = False


    def decode(self, data: bytes) -> CrawlerResponse:
        return CrawlerResponse.model_validate_json(data)
        
    def encode(self,data: DisasterAnalyzerResponse) -> bytes:
        return data.model_dump_json().encode()

    async def process(self, data: CrawlerResponse):
        try:
            result = await self._analyzer.analyze(data.message)
            if result:
                return result
        except Exception as e:
            self.__log.error(e)
