
import socket

from collections.abc import Callable
from analyzer import DisasterAnalyzer
from fogverse.general import ParallelRunnable
from master.contract import CloudDeployConfigs, TopicDeploymentConfig
from master.master import ConsumerAutoScaler, ProducerObserver
from .contract import DisasterAnalyzerResponse
from crawler.contract import CrawlerResponse
from fogverse import Producer, Consumer
from fogverse.fogverse_logging import get_logger
from typing import Any, Optional


class AnalyzerProducer(Consumer, Producer):

    def __init__(self, 
                 producer_topic: str, 
                 producer_servers: list[str] | str, 
                 consumer_topic: str, 
                 consumer_servers: str,
                 consumer_group_id: str,
                 classifier_model: DisasterAnalyzer,
                 consumer_auto_scaler: Optional[ConsumerAutoScaler],
                 producer_observer: ProducerObserver,
                 topic_deployment_config: TopicDeploymentConfig
                ):

        self.consumer_topic =  consumer_topic
        self.consumer_servers = consumer_servers
        self.producer_topic = producer_topic 
        self.producer_servers = producer_servers
        self._classifier_model = classifier_model
        self.group_id = consumer_group_id
        self._log = get_logger(name=self.__class__.__name__)
        self.auto_decode = False

        Producer.__init__(self)
        Consumer.__init__(self)

        self._topic_deployment_config = topic_deployment_config
        self._consumer_auto_scaler = consumer_auto_scaler
        self._observer = producer_observer
        self._closed = False

        self.client_id = socket.gethostname()


    def decode(self, data: bytes) -> CrawlerResponse:
        return CrawlerResponse.model_validate_json(data)
        
    def encode(self,data: DisasterAnalyzerResponse) -> bytes:
        return data.model_dump_json().encode()

    async def _start(self):
        if self._consumer_auto_scaler:
            await self._consumer_auto_scaler.async_start(
                consumer=self.consumer,
                consumer_group=self.group_id,
                consumer_topic=self.consumer_topic,
                producer=self.producer,
            )
        else:
            await super()._start()

        await self._observer.send_input_output_ratio_pair(
            source_topic=self.consumer_topic,
            target_topic=self.producer_topic,
            topic_configs=self._topic_deployment_config,
            send = lambda x, y: self.producer.send(topic=x, value=y)
        )


    async def process(self, data: CrawlerResponse):
        try:
            message_is_disaster = self._classifier_model.analyze("is_disaster", [data.message])[0]
            
            if message_is_disaster == "0":
                return DisasterAnalyzerResponse(
                    is_disaster=message_is_disaster,
                    text=data.message
                )

            keyword_result = self._classifier_model.analyze("keyword", [data.message])

            if keyword_result:
                return DisasterAnalyzerResponse(
                   keyword=keyword_result[0],
                   is_disaster=message_is_disaster,
                   text=data.message
                )

        except Exception as e:
            self._log.error(e)

    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        result = await super().send(data, topic, key, headers, callback)
        await self._observer.send_total_successful_messages(
            target_topic=self.producer_topic,
            send = lambda x, y: self.producer.send(topic=x, value=y),
            total_messages = 1
        )
        return result

class ParallelAnalyzerJobService:
    def __init__(self, runnable: ParallelRunnable, producer_observer: ProducerObserver):
        self.runnable = runnable
        self.producer_observer = producer_observer

    def start(self):
        self.runnable.run(self.producer_observer.send_total_successful_messages)

