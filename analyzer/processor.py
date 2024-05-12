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

    def process(self, messages: list[Message]) -> list[bytes]:
        try:
            crawler_response = list(map(lambda message : CrawlerResponse.model_validate_json(message.value()), messages))
            crawler_messages = [response.message  for response in crawler_response]           

            disaster_messages = self.analyzer.analyze("is_disaster", crawler_messages)
            keyword_messages = self.analyzer.analyze("keyword", crawler_messages)

            return [
                DisasterAnalyzerResponse(
                    text=crawler_response[i].message,
                    is_disaster=disaster_messages[i],
                    keyword=keyword_messages[i],
                    crawler_timestamp=crawler_response[i].timestamp,
                    analyzer_timestamp=time.time()
                ).model_dump_json().encode() 
                for i in range(len(crawler_response)
            )]

        except Exception as e:
            self.logger.error(e)
            return []
