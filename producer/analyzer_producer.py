from analyzer import DisasterAnalyzer
from fogverse import Producer, Consumer
from fogverse.fogverse_logging import get_logger

class AnalyzerProducer(Consumer, Producer):

    def __init__(self, 
                 producer_topic: str, 
                 producer_servers: list[str] | str, 
                 consumer_topic: str, 
                 consumer_servers: str,
                 analyzer: DisasterAnalyzer):

        self.consumer_topic =  consumer_topic
        self.consumer_servers = consumer_servers
        self.producer_topic = producer_topic 
        self.producer_servers = producer_servers
        self._analyzer = analyzer
        self.__log = get_logger(name=self.__class__.__name__)

        Producer.__init__(self)
        Consumer.__init__(self)
        self._closed = False


    # TODO: change data type to crawler response
    async def process(self, data: str):
        try:
            result = await self._analyzer.analyze(data)
            if result:
                return str(result.is_disaster)
        except Exception as e:
            self.__log.error(e)
