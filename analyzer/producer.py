
from analyzer import DisasterAnalyzer
from .contract import DisasterAnalyzerResponse
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
                 classifier_model: DisasterAnalyzer,
                ):

        self.consumer_topic =  consumer_topic
        self.consumer_servers = consumer_servers
        self.producer_topic = producer_topic 
        self.producer_servers = producer_servers
        self._classifier_model = classifier_model
        self.group_id = consumer_group_id
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
        return DisasterAnalyzerResponse(
            is_disaster="0",
            text=data.message
        )
        # try:
        #     message_is_disaster = await self._classifier_model.analyze("is_disaster", data.message)
            
        #     if message_is_disaster == "0":
        #         return DisasterAnalyzerResponse(
        #             is_disaster=message_is_disaster,
        #             text=data.message
        #         )

        #     keyword_result = await self._classifier_model.analyze("keyword", data.message)

        #     if keyword_result:
        #         return DisasterAnalyzerResponse(
        #            keyword=keyword_result,
        #            is_disaster=message_is_disaster,
        #            text=data.message
        #         )

        # except Exception as e:
        #     self.__log.error(e)
