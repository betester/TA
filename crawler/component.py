
import os

from master.master import ProducerObserver

from .crawler import MockUpCrawler
from ..model.crawler_contract import CrawlerResponse
from .handler import CrawlerProducer
from fogverse.util import get_config

class CrawlerComponent:


    def __init__(self):
        self._producer_servers =  str(get_config("CRAWLER_ANALYZER_SERVERS", self, "localhost:9092"))
        self._producer_topic =  str(get_config("CRAWLER_PRODUCER_TOPIC", self, "xi"))
        self._consumer_group_id = str(get_config("CRAWLER_CONSUMER_GROUP_ID", self, "crawler"))

    def __kaggle_parser(self, row: list[str]) -> CrawlerResponse:
        return CrawlerResponse(
            message=row[3],
            source="kaggle data"
        )

    def __read_files(self, directory_path: str):
        csv_files = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith('.csv')]
        return [open(csv_file) for csv_file in csv_files]

    def mock_disaster_crawler(self, directory_path: str, observer: ProducerObserver):

        crawler = MockUpCrawler(
            self.__kaggle_parser, 
            *self.__read_files(directory_path)
        )
        

        return CrawlerProducer(
            producer_topic=self._producer_topic,
            consumer_group_id=self._consumer_group_id,
            producer_servers=self._producer_servers,
            crawler=crawler,
            observer=observer
        )
