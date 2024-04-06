import asyncio
from fogverse import Producer, Profiling
from model.crawler_contract import CrawlerResponse
from fogverse.util import get_config
import socket

class DiscordProducer(Producer, Profiling):
    def __init__(self, messages, producer_observer):
        self.producer_topic = str(get_config("DISCORD_PRODUCER_TOPIC", self, "analyze_v1"))
        self.producer_servers = str(get_config("DISCORD_PRODUCER_SERVERS", self, "localhost:9092"))
        self.group_id = str(get_config("DISCORD_CONSUMER_GROUP_ID", self, "crawler"))
        self.messages = messages
        self._closed = False
        self._observer = producer_observer
        self.client_id = socket.gethostname()
        Producer.__init__(self)
        Profiling.__init__(self, name='discord-logs', dirname='discord-logs')

    def encode(self, data: CrawlerResponse) -> bytes:
        return data.model_dump_json().encode()

    async def receive(self):
        try:
            data = await self.messages.get()
            return CrawlerResponse(
                message=data,
                source="discord"
            )
        
        except asyncio.QueueEmpty:
            return None
    
    def encode(self, data: CrawlerResponse) -> bytes:
        return data.model_dump_json().encode()
    
    async def send(self, data: CrawlerResponse):
        result = await super().send(data=data)
        await self._observer.send_total_successful_messages(
            target_topic=self.producer_topic,
            send = lambda x, y: self.producer.send(topic=x, value=y),
            total_messages = 1
        )
        return result
