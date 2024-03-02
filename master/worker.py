
from aiokafka.client import asyncio
import logging

from aiokafka.conn import functools

class StatisticWorker:

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
        self.logger = logging.getLogger(self.__class__.__name__)

        logger_format = logging.Formatter(fmt='[%(asctime)s][%(levelname)s][%(name)s] %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(logger_format)
        self.logger.addHandler(handler)

        self._stop = False

    
    def on_receive_topic(self, topic: str, total_messages: int):
        topic_current_count = self._topics_current_count.get(topic, 0)
        topic_current_count += total_messages
        self._topics_current_count[topic] = topic_current_count

    def _add_observed_topic_counts(self, topic, total_counts: int):
        observed_counts = self._topics_observed_counts.get(topic, [])

        if len(observed_counts) >= self._maximum_seconds:
            observed_counts.pop(0)

        observed_counts.append(total_counts)
        self._topics_observed_counts[topic] = observed_counts


    async def start(self):

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
            self.logger.info(e)
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
            self.logger.info(e)
            return 0
    
    def stop(self):
        self._stop = True
