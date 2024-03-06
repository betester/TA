import asyncio
from fogverse import Producer, Profiling
from crawler import CrawlerResponse
from fogverse.util import get_config

class DiscordProducer(Producer, Profiling):
    def __init__(self, messages):
        self.producer_topic = str(get_config("DISCORD_PRODUCER_TOPIC", self, "analyze_v1"))
        self.producer_servers = str(get_config("DISCORD_PRODUCER_SERVERS", self, "localhost:9092"))
        self.group_id = str(get_config("DISCORD_CONSUMER_GROUP_ID", self, "crawler"))
        self.messages = messages
        self._closed = False
        Producer.__init__(self)
        Profiling.__init__(self, name='discord-logs', dirname='discord-logs')

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