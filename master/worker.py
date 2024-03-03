
from collections.abc import Callable, Coroutine
from typing import Any
from aiokafka.client import asyncio

from aiokafka.conn import functools
from master.contract import InputOutputThroughputPair, MachineConditionData, MasterObserver, TopicStatistic

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
        print("Starting statistic worker")
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
            print(e)
            return 0

    def get_topic_standard_deviation(self, topic: str) -> float:
        try:
            topic_observed_count = self._topics_observed_counts[topic]
            total_observed_counts = sum(topic_observed_count)
            observed_counts_size = len(topic_observed_count)
            avg_observed_counts = total_observed_counts/(observed_counts_size)

            total_diff_squared_observed_counts = functools.reduce(
                lambda cumulative_sum, observed_count: (observed_count - avg_observed_counts)**2 + cumulative_sum,
                topic_observed_count
            )

            topic_variance = total_diff_squared_observed_counts/observed_counts_size

            return topic_variance**(1/2)

        except Exception as e:
            print(e)
            return 0
    
    def stop(self):
        self._stop = True

class InputOutputRatioWorker(MasterObserver):

    def __init__(
            self,
            refresh_rate_second: float,
            input_output_ratio_threshold: float,
            below_threshold_callback: Callable[[str, int], Coroutine[Any, Any, bool]]
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

        self._stop = False
        
    
    def on_receive(self, data: InputOutputThroughputPair | MachineConditionData):
        if isinstance(data, InputOutputThroughputPair):
            target_topics = self._topics_throughput_pair.get(data.source_topic, [])
            target_topics.append(data.target_topic)
            self._topics_throughput_pair[data.source_topic] = target_topics
        else:
            topic_current_count = self._topics_current_count.get(data.target_topic, 0)
            topic_current_count += data.total_messages
            self._topics_current_count[data.target_topic] = topic_current_count

    async def start(self):
        print("Starting input output ratio worker")
        while not self._stop:
            await asyncio.sleep(self._refresh_rate_second)
            for source_topic, target_topics in self._topics_throughput_pair.items():
                source_topic_throughput = self._topics_current_count.get(source_topic, 0)
                for target_topic in target_topics:
                    target_topic_throughput = self._topics_current_count.get(target_topic, 0)
                    throughput_ratio = target_topic_throughput/max(source_topic_throughput, 1)

                    if throughput_ratio == target_topic_throughput:
                        print(f"Source topic {source_topic} throughput is {source_topic_throughput}, the machine might be dead")

                    if throughput_ratio < self._input_output_ratio_threshold:
                        await self._below_threshold_callback(target_topic, target_topic_throughput)

            for topic, throughput in self._topics_current_count.items():
                print(f"Topic {topic} total message in {self._topics_current_count} seconds: {throughput}")
                self._topics_current_count[topic] = 0
            
    def stop(self):
        self._stop = True 
