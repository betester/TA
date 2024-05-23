import os
import time

from master.master import ProducerObserver

from .crawler import MockUpCrawler
from model.crawler_contract import CrawlerResponse
from .handler import CrawlerProducer
from fogverse.util import get_config

class CrawlerComponent:


    def __init__(self):
        self._producer_servers =  str(get_config("CRAWLER_ANALYZER_SERVERS", self, "localhost:9092"))
        self._producer_topic =  str(get_config("CRAWLER_PRODUCER_TOPIC", self, "xi2"))
        self._consumer_group_id = str(get_config("CRAWLER_CONSUMER_GROUP_ID", self, "crawler"))
        self._metadata_max_age_ms = int(str(get_config("METADATA_MAX_AGE_MS", self, 5000)))
        self._crawler_delay = float(str(get_config("CRAWLER_DELAY", self, 0.1)))

        self._spam_delay = float(str(get_config("SPAM_DELAY", self, 600)))
        self._spam_rate = float(str(get_config("SPAM_RATE", self, 0.01)))
        self._spam_duration = float(str(get_config("SPAM_DURATION", self, 60)))

        self._use_spam = int(str(get_config("USE_SPAM", self, 0)))

    def __kaggle_parser(self, row: list[str]) -> CrawlerResponse:
        return CrawlerResponse(
            message=row[3],
            source="kaggle data",
            timestamp=time.time()
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
            observer=observer,
            metadata_max_age_ms=self._metadata_max_age_ms,
            crawler_delay=self._crawler_delay,
            spam_delay=self._spam_delay,
            spam_rate=self._spam_rate,
            spam_duration=self._spam_duration
        )
