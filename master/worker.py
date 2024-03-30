
import logging

from collections.abc import Callable, Coroutine
from typing import Any
from uuid import uuid4
from aiokafka.client import asyncio

from aiokafka.conn import functools
from fogverse.fogverse_logging import FogVerseLogging, get_logger
from master.contract import InputOutputThroughputPair, MachineConditionData, MasterObserver, TopicStatistic
from master.master import AutoDeployer

class StatisticWorker(MasterObserver, TopicStatistic):

    def __init__(self, maximum_seconds: int, refresh_rate: float = 1):
        '''
        A sliding window statistic approach for calculating the statistics of topic total throughput.
        maximum_seconds : int (seconds) = maximum amount of seconds for the sliding window time.
        refresh_rate: int (seconds) =  how frequent the worker will refresh the observed topic throughput, 
        by default it will refresh each second.
        '''
        assert maximum_seconds > 0
        assert refresh_rate > 0

        self._maximum_seconds = maximum_seconds
        self._refresh_rate = refresh_rate   
        self._topics_current_count: dict[str, int] = {} 
        self._topics_observed_counts: dict[str, list[int]] = {}
        self._logger = get_logger(name=self.__class__.__name__)

        self._stop = False

    
    def on_receive(self, data: InputOutputThroughputPair | MachineConditionData):
        if isinstance(data, InputOutputThroughputPair):
            return
    
        topic_current_count = self._topics_current_count.get(data.target_topic, 0)
        topic_current_count += data.total_messages
        self._topics_current_count[data.target_topic] = topic_current_count

    def _add_observed_topic_counts(self, topic, total_counts: int):
        observed_counts = self._topics_observed_counts.get(topic, [])

        if len(observed_counts) >= self._maximum_seconds:
            observed_counts.pop(0)

        observed_counts.append(total_counts)
        self._topics_observed_counts[topic] = observed_counts


    async def start(self):
        self._logger.info("Starting statistic worker")
        while not self._stop:
            await asyncio.sleep(self._refresh_rate)

            for topic, current_counts in self._topics_current_count.items():
                self._add_observed_topic_counts(topic, current_counts)
                self._topics_current_count[topic] = 0 

    def get_topic_mean(self, topic: str) -> float:
        try:
            topic_observed_count = self._topics_observed_counts[topic]
            total_observed_counts = sum(topic_observed_count)
            return total_observed_counts/len(topic_observed_count)
        except Exception as e:
            self._logger.error(e)
            return 0

    def get_topic_standard_deviation(self, topic: str) -> float:
        try:
            topic_observed_count = self._topics_observed_counts[topic]
            total_observed_counts = sum(topic_observed_count)
            observed_counts_size = len(topic_observed_count)
            avg_observed_counts = total_observed_counts/(observed_counts_size)

            total_diff_squared_observed_counts = functools.reduce(
                lambda cumulative_sum, observed_count: (observed_count - avg_observed_counts)**2 + cumulative_sum,
                topic_observed_count,
                0
            )

            topic_variance = total_diff_squared_observed_counts/observed_counts_size

            return topic_variance**(1/2)

        except Exception as e:
            self._logger.error(e)
            return 0
    
    async def stop(self):
        self._stop = True

class ProfillingWorker(MasterObserver):

    def __init__(self, total_machine_deployed : Callable[[str], int]):

        self._total_machine_deployed = total_machine_deployed
        self.__headers = [
            "topic",
            "topic_throughput_per_second"
        ]

        self._fogverse_logger = FogVerseLogging(
            name=f"{self.__class__.__name__}-{uuid4()}",
            csv_header=self.__headers,
            level= logging.INFO + 2
        )

        self._topics_current_count: dict[str, int] = {} 
        self._stop = False

    def on_receive(self, data: InputOutputThroughputPair | MachineConditionData):
        if isinstance(data, MachineConditionData):
            topic_current_count = self._topics_current_count.get(data.target_topic, 0)
            topic_current_count += data.total_messages
            self._topics_current_count[data.target_topic] = topic_current_count

    def _csv_message(self, data : dict[str, Any]):
        message = []
        
        for header in self.__headers:
            message.append(data.get(header, ""))
    
        return message

    def _flush(self):
        for topic in self._topics_current_count:
            self._topics_current_count[topic] = 0

    async def start(self):
        self._fogverse_logger.std_log(f"Starting {self.__class__.__name__}")

        while not self._stop:
            try:
                await asyncio.sleep(1)            
                self._fogverse_logger.std_log(self._topics_current_count)
                for topic, topic_current_count in self._topics_current_count:
                    log = self._csv_message({
                        "topic" : topic,
                        "topic_throughput_per_second" : topic_current_count,
                        "topic_machine_deployed" : str(self._total_machine_deployed(topic))
                    })

                    self._fogverse_logger.csv_log(log)

                self._flush()

            except Exception as e:
                self._fogverse_logger.std_log(e)


    async def stop(self):
        self._stop = True

class InputOutputRatioWorker(MasterObserver):

    def __init__(
            self,
            refresh_rate_second: float,
            input_output_ratio_threshold: float,
            below_threshold_callback: Callable[[str, float, str], Coroutine[Any, Any, bool]]
        ):
        '''
        Worker that helps for counting input output ratio of topic
        refresh_rate_second (second) : How frequent the worker will count the ratio between input and output ratio.
        input_output_ratio_threshold : Ranging from 0.0 to 1.0, if the ratio is below the threshold and fulfill certain criteria, it will deploy a new instance 
        '''

        assert refresh_rate_second > 0
        assert input_output_ratio_threshold > 0
        assert input_output_ratio_threshold <= 1
        
        self._refresh_rate_second = refresh_rate_second
        self._input_output_ratio_threshold = input_output_ratio_threshold
        self._below_threshold_callback = below_threshold_callback

        self._topics_current_count: dict[str, int] = {} 
        self._topics_throughput_pair: dict[str, list[str]] = {}
        self._logger = get_logger(name=self.__class__.__name__)

        self._stop = False
        
    
    def on_receive(self, data: InputOutputThroughputPair | MachineConditionData):
        if isinstance(data, InputOutputThroughputPair):
            self._logger.info(f"Received input output through pair data: {data}")
            target_topics = self._topics_throughput_pair.get(data.source_topic, [])
            target_topics.append(data.target_topic)
            self._topics_throughput_pair[data.source_topic] = target_topics
        else:
            topic_current_count = self._topics_current_count.get(data.target_topic, 0)
            topic_current_count += data.total_messages
            self._topics_current_count[data.target_topic] = topic_current_count

    def _flush(self):
        for topic, throughput in self._topics_current_count.items():
            self._logger.info(f"Topic {topic} total message in {self._refresh_rate_second} seconds: {throughput}")
            self._topics_current_count[topic] = 0

    async def start(self):
        self._logger.info("Starting input output ratio worker")
        while not self._stop:
            await asyncio.sleep(self._refresh_rate_second)
            for source_topic, target_topics in self._topics_throughput_pair.items():
                source_topic_throughput = self._topics_current_count.get(source_topic, 0)
                for target_topic in target_topics:
                    target_topic_throughput = self._topics_current_count.get(target_topic, 0)
                    throughput_ratio = target_topic_throughput/max(source_topic_throughput, 1)

                    if throughput_ratio == target_topic_throughput:
                        self._logger.info(f"Source topic {source_topic} throughput is {source_topic_throughput}, the machine might be dead")

                    self._logger.info(f"Ratio between topic {target_topic} and {source_topic} is: {throughput_ratio}")
                    if throughput_ratio < self._input_output_ratio_threshold:
                        self._logger.info(f"{throughput_ratio} is less than threshold: {self._input_output_ratio_threshold}")
                        await self._below_threshold_callback(source_topic, source_topic_throughput/self._refresh_rate_second, target_topic)
            
            self._flush()
            
    async def stop(self):
        self._stop = True 
