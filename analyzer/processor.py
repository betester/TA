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

            message_is_disaster = self.analyzer.analyze("is_disaster", crawler_result.message)

            if message_is_disaster == "0":
                return DisasterAnalyzerResponse(
                        is_disaster=message_is_disaster,
                        text=crawler_result.message
                    ).model_dump_json().encode()

            keyword_result = self.analyzer.analyze("keyword", crawler_result.message)

            if keyword_result:
                return DisasterAnalyzerResponse(
                        keyword=keyword_result,
                        is_disaster=message_is_disaster,
                        text=crawler_result.message
                    ).model_dump_json().encode()

            return DisasterAnalyzerResponse().model_dump_json().encode()

        except Exception as e:
            self.logger.error(e)
            return DisasterAnalyzerResponse().model_dump_json().encode()
