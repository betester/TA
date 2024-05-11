from fogverse import Consumer
from fogverse.fogverse_logging import get_logger, FogVerseLogging
from fogverse.util import get_config
from model.analyzer_contract import DisasterAnalyzerResponse
from client.notif_client import NotificationClient
import os
import logging
from uuid import uuid4
import time

class NotificationConsumer(Consumer):
    def __init__(self, producer_observer, notification_client: NotificationClient):
        self.consumer_topic = str(get_config("NOTIF_CONSUMER_TOPIC", self, "client_v7"))
        self.consumer_servers = str(get_config("NOTIF_CONSUMER_SERVERS", self, "localhost:9092"))
        self.group_id = str(get_config("NOTIF_CONSUMER_GROUP_ID", self, "consumer"))
        self._closed = False
        self.__client = notification_client
        self._observer = producer_observer
        self.__headers = [
            "keyword",
            "is_disaster",
            "text",
            "crawler_timestamp",
            "analyzer_timestamp",
            "notification_timestamp"
        ]
        self._fogverse_logger = FogVerseLogging(
            name=f"{self.__class__.__name__}-{uuid4()}",
            dirname="notification-logs",
            csv_header=self.__headers,
            level= logging.INFO + 2
        )
        Consumer.__init__(self)
        self._fogverse_logger.std_log(f"NotificationConsumer initialized with topic: {self.consumer_topic}")


    def decode(self, data: bytes) -> DisasterAnalyzerResponse:
        return DisasterAnalyzerResponse.model_validate_json(data)
    
    def encode(self, data) -> str:
        return data

    def process(self, data: DisasterAnalyzerResponse):
        return super().process(data)
    
    async def send(self, data: DisasterAnalyzerResponse, topic=None, key=None, headers=None, callback=None):
        self._fogverse_logger.std_log(f"Data: {data}")
        if int(data.is_disaster):
            try:
                self.__client.send_notification(data.text)
                await self._observer.send_total_successful_messages(
                    target_topic=self.producer_topic,
                    send = lambda x, y: self.producer.send(topic=x, value=y),
                    total_messages = 1
                )
            except Exception as e:
                self.__log.error(f"Error sending notification: {e}")

        self._fogverse_logger.csv_log(
            [data.keyword, data.is_disaster, data.text, data.crawler_timestamp, data.analyzer_timestamp, time.time()]
        )
        return data
