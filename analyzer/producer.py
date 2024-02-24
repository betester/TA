
from collections.abc import Callable
from analyzer import DisasterAnalyzer
from fogverse.general import ParallelRunnable
from master.master import ConsumerAutoScaler, ProducerObserver
from .contract import DisasterAnalyzerResponse
from crawler.contract import CrawlerResponse
from fogverse import Producer, Consumer
from fogverse.fogverse_logging import get_logger
from typing import Optional
from confluent_kafka import Producer as ConfluentProducer


class AnalyzerProducer(Consumer, Producer):

    def __init__(self, 
                 producer_topic: str, 
                 producer_servers: list[str] | str, 
                 consumer_topic: str, 
                 consumer_servers: str,
                 consumer_group_id: str,
                 classifier_model: DisasterAnalyzer,
                 consumer_auto_scaler: Optional[ConsumerAutoScaler],
                 producer_observer: ProducerObserver
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

        self._consumer_auto_scaler = consumer_auto_scaler
        self._observer = producer_observer
        self._closed = False


    def decode(self, data: bytes) -> CrawlerResponse:
        return CrawlerResponse.model_validate_json(data)
        
    def encode(self,data: DisasterAnalyzerResponse) -> bytes:
        return data.model_dump_json().encode()

    async def _start(self):
        if self._consumer_auto_scaler:
            await self._consumer_auto_scaler._start(
                consumer=self.consumer,
                consumer_group=self.group_id,
                consumer_topic=self.consumer_topic,
                producer=self.producer,
            )
        else:
            await super()._start()

        self._observer.send_input_output_ratio_pair(
            source_topic=self.consumer_topic,
            target_topic=self.producer_topic,
            producer=self.producer
        )


    async def process(self, data: CrawlerResponse):
        try:
            message_is_disaster = self._classifier_model.analyze("is_disaster", data.message)
            
            if message_is_disaster == "0":
                return DisasterAnalyzerResponse(
                    is_disaster=message_is_disaster,
                    text=data.message
                )

            keyword_result = self._classifier_model.analyze("keyword", data.message)

            if keyword_result:
                return DisasterAnalyzerResponse(
                   keyword=keyword_result,
                   is_disaster=message_is_disaster,
                   text=data.message
                )

        except Exception as e:
            self.__log.error(e)

    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        result = await super().send(data, topic, key, headers, callback)
        self._observer.send_success_send_timestamp(target_topic=self.producer_topic, producer=self.producer)
        return result

class ParallelAnalyzerJobService:
    #TODO: add observer
    def __init__(self, runnable: ParallelRunnable):
        self.runnable = runnable

    def start(self, on_producer_complete: Callable[[str, ConfluentProducer], None]):
        self.runnable.run(on_producer_complete)

