
import os

from crawler import CrawlerResponse
from crawler.crawler import MockUpCrawler
from producer.crawler_proucer import CrawlerProducer


class KaggleCrawlerComponent:

    def __init__(self, directory_path: str):
        self._crawler = MockUpCrawler(
            parser=self.__kaggle_parser, 
            *self.__read_files(directory_path)
        )
        self._crawler_producer = CrawlerProducer(
            producer_topic="testing_topic",
            producer_servers="localhost:9092",
            crawler=self._crawler
        )

    def __kaggle_parser(self, row: list[str]) -> CrawlerResponse:
        return CrawlerResponse(
            message=row[3],
            source="kaggle data"
        )

    def __read_files(self, directory_path: str):
        csv_files = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith('.csv')]
        return [open(csv_file) for csv_file in csv_files]
    
    @property
    def crawler_producer(self):
        return self._crawler_producer
