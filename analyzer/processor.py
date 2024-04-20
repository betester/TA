from confluent_kafka import Message
from model.analyzer_contract import DisasterAnalyzer, DisasterAnalyzerResponse
from model.crawler_contract import CrawlerResponse
from fogverse.base import Processor
from fogverse.fogverse_logging import get_logger


class AnalyzerProcessor(Processor):

    def __init__(self, disaster_analyzer: DisasterAnalyzer):
        self.analyzer = disaster_analyzer
        self.logger = get_logger()

    def process(self, messages: list[Message]) -> list[bytes]:
        try:
            crawler_results = list(map(lambda message : CrawlerResponse.model_validate_json(message.value()).message, messages))
            disaster_messages = self.analyzer.analyze("is_disaster", crawler_results)
            keyword_messages = self.analyzer.analyze("keyword", crawler_results)

            return [
                DisasterAnalyzerResponse(
                    text=crawler_results[i],
                    is_disaster=disaster_messages[i],
                    keyword=keyword_messages[i]
                ).model_dump_json().encode() 
                for i in range(len(crawler_results)
            )]

        except Exception as e:
            self.logger.error(e)
            return []
