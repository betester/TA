from typing import Any
from confluent_kafka import Message
from model.analyzer_contract import DisasterAnalyzer, DisasterAnalyzerResponse
from model.crawler_contract import CrawlerResponse
from fogverse.base import Processor
from fogverse.fogverse_logging import get_logger
import time

class AnalyzerProcessor(Processor):

    def __init__(self, disaster_analyzer: DisasterAnalyzer):
        self.analyzer = disaster_analyzer
        self.logger = get_logger(name=self.__class__.__name__)

    def process(self, data: list[CrawlerResponse]) -> list[Any]:
        try:
            # should be 60 per minute
            time.sleep(1)
            return [
                DisasterAnalyzerResponse(
                    text="Bisa diringankan dengan peluuuuuuuuuuuuuuuuuuuuuuuuk",
                    is_disaster="1",
                    keyword="PELUKKKK",
                    crawler_timestamp=time.time(),
                    analyzer_timestamp=time.time()
                ) 
                for i in range(len(data)
            )]

        except Exception as e:
            self.logger.error(e)
            return []
