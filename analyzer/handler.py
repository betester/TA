import socket
import time

from analyzer import DisasterAnalyzer
from fogverse.base import Processor
from fogverse.general import ParallelRunnable
from master.contract import TopicDeploymentConfig
from master.master import ConsumerAutoScaler, ProducerObserver
from model.analyzer_contract import DisasterAnalyzerResponse
from model.crawler_contract import CrawlerResponse
from fogverse import ParallelConsumer, ParallelProducer
from fogverse.fogverse_logging import get_logger
from typing import Optional
from asyncio import sleep, create_task

import torch.multiprocessing

torch.multiprocessing.set_sharing_strategy('file_system')

class AnalyzerProducer(ParallelConsumer, ParallelProducer):

    def __init__(self, 
                 producer_topic: str, 
                 producer_servers: list[str] | str, 
                 consumer_topic: str, 
                 consumer_servers: str,
                 consumer_group_id: str,
                 classifier_model: DisasterAnalyzer,
                 consumer_auto_scaler: Optional[ConsumerAutoScaler],
                 producer_observer: ProducerObserver,
                 topic_deployment_config: TopicDeploymentConfig,
                 analyzer_processor : Processor
                ):

        self.consumer_topic =  consumer_topic
        self.consumer_servers = consumer_servers
        self.producer_topic = producer_topic 
        self.producer_servers = producer_servers
        self._classifier_model = classifier_model
        self.group_id = consumer_group_id
        self._log = get_logger(name=self.__class__.__name__)
        self.auto_decode = False

        ParallelProducer.__init__(self)
        ParallelConsumer.__init__(self)

        self.processor = analyzer_processor
        self._topic_deployment_config = topic_deployment_config
        self._consumer_auto_scaler = consumer_auto_scaler
        self._observer = producer_observer
        self._closed = False

        self.send_rate = 0.1

        self.client_id = socket.gethostname()


    def decode(self, data: bytes) -> CrawlerResponse:
        return CrawlerResponse.model_validate_json(data)
        
    def encode(self,data: DisasterAnalyzerResponse) -> bytes:
        return data.model_dump_json().encode()

    async def slow_down(self):

        slow_down_delay : int = 600
        slow_down_rate  : float = 0.5
        slow_down_duration : int = 60
        self._log.info("Inititating slow down")
        await sleep(slow_down_delay)
        self._log.info("Slowing down")
        previous_rate = self.send_rate
        self.send_rate = slow_down_rate
        await sleep(slow_down_duration)
        self._log.info("Slowing down finished")
        self.send_rate = previous_rate


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

        create_task(self.slow_down())


    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        result = await super().send(data, topic, key, headers, callback)
        await self._observer.send_total_successful_messages_async(
            target_topic=self.producer_topic,
            send = self.producer.send,
            total_messages = 1
        )
        return result

class ParallelAnalyzerJobService:
    def __init__(self, runnable: ParallelRunnable):
        self.runnable = runnable

    def start(self):
        self.runnable.run()

