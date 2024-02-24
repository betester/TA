import time
from confluent_kafka import Message
from analyzer.contract import DisasterAnalyzer, DisasterAnalyzerResponse
from crawler.contract import CrawlerResponse
from fogverse.base import Processor
from fogverse.fogverse_logging import get_logger


class AnalyzerProcessor(Processor):

    def __init__(self, disaster_analyzer: DisasterAnalyzer):
        self.analyzer = disaster_analyzer
        self.logger = get_logger()

    def process(self, message: Message) -> bytes:
        try:
            crawler_result = CrawlerResponse.model_validate_json(message.value())
            self.logger.info(crawler_result)
            time.sleep(0.1)
            return DisasterAnalyzerResponse().model_dump_json().encode()

        except Exception as e:
            self.logger.error(e)
            return DisasterAnalyzerResponse().model_dump_json().encode()
