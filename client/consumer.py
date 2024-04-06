from fogverse import Consumer
from fogverse.fogverse_logging import get_logger
from fogverse.util import get_config
from model.analyzer_contract import DisasterAnalyzerResponse
from client.notif_client import NotificationClient
import os

class NotificationConsumer(Consumer):
    def __init__(self, notification_client: NotificationClient):
        self.consumer_topic = str(get_config("NOTIF_CONSUMER_TOPIC", self, "client_v6"))
        self.consumer_servers = str(get_config("NOTIF_CONSUMER_SERVERS", self, "localhost:9092"))
        self.group_id = str(get_config("NOTIF_CONSUMER_GROUP_ID", self, "consumer"))
        self._closed = False
        self.__client = notification_client
        self.__log = get_logger(name=self.__class__.__name__)
        Consumer.__init__(self)
        self.__log.info(f"NotificationConsumer initialized with topic: {self.consumer_topic}")

    def decode(self, data: bytes) -> DisasterAnalyzerResponse:
        return DisasterAnalyzerResponse.model_validate_json(data)
    
    def encode(self, data) -> str:
        return data

    def process(self, data: DisasterAnalyzerResponse):
        return super().process(data)
    
    async def send(self, data: DisasterAnalyzerResponse, topic=None, key=None, headers=None, callback=None):
        self.__log.info(f"Data: {data}")
        if int(data.is_disaster):
            try:
                self.__client.send_notification(data.text)

            except Exception as e:
                self.__log.error(f"Error sending notification: {e}")
        return data
