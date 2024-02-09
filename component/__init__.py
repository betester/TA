
import os

from crawler import CrawlerResponse
from crawler.crawler import MockUpCrawler
from fogverse.util import get_config
from producer.crawler_proucer import CrawlerProducer
from producer.analyzer_producer import AnalyzerProducer
from analyzer.analyzer import DisasterAnalyzerImpl


class Component:

    def __init__(self):
        self._crawler_topic =  str(get_config("CRAWLER_TOPIC", self, "analyze"))
        self._crawler_consumer_group_id = str(get_config("CRAWLER_CONSUMER_GROUP_ID", self, "crawler"))
        self._analyzer_topic = str(get_config("ANALYZER_TOPIC", self, "analyzer"))
        self._analyzer_consumer_group_id = str(get_config("ANALYZER_CONSUMER_GROUP_ID", self, "analyzer"))

        self._kafka_server =  str(get_config("KAFKA_SERVER", self, "localhost:9092"))


    def __kaggle_parser(self, row: list[str]) -> CrawlerResponse:
        return CrawlerResponse(
            message=row[3],
            source="kaggle data"
        )

    def __read_files(self, directory_path: str):
        csv_files = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith('.csv')]
        return [open(csv_file) for csv_file in csv_files]
    
    def crawler_producer(self, directory_path: str):

        self._crawler = MockUpCrawler(
            self.__kaggle_parser, 
            *self.__read_files(directory_path)
        )
        self._crawler_producer = CrawlerProducer(
            producer_topic=self._crawler_topic,
            consumer_group_id=self._crawler_consumer_group_id,
            producer_servers=self._kafka_server,
            crawler=self._crawler
        )
        return self._crawler_producer
    
    def analyzer_producer(self, model_source: str):
        analyzer = DisasterAnalyzerImpl(model_source)
        analyzer_producer = AnalyzerProducer(
            producer_topic='client_v2',
            producer_servers=self._kafka_server, 
            consumer_topic=self._crawler_topic, 
            consumer_servers=self._kafka_server, 
            analyzer=analyzer,
            consumer_group_id=self._analyzer_consumer_group_id
        )

        return analyzer_producer
