import discord
from fogverse.fogverse_logging import get_logger
from fogverse import Producer, Profiling
from crawler import CrawlerResponse
import asyncio
import threading

TOKEN = 'redacted'
PRODUCER_TOPIC = "analyze_v1"
PRODUCER_SERVERS = "localhost:9092"
CONSUMER_GROUP_ID = "crawler"

class DiscordProducer(Producer, Profiling):
    def __init__(self, client, messages):
        self.producer_topic = PRODUCER_TOPIC 
        self.producer_servers = PRODUCER_SERVERS
        self.group_id = CONSUMER_GROUP_ID
        self.messages = messages
        self._client = client
        self._closed = False
        Producer.__init__(self)
        Profiling.__init__(self, name='discord-logs', dirname='discord-logs')

    async def receive(self):
        if self.messages:
            return CrawlerResponse(
                message=self.messages.pop(0),
                source="discord"
            )
        return None
    
    def encode(self, data: CrawlerResponse) -> bytes:
        return data.model_dump_json().encode()

class Jigglypuff(discord.Client):
    def __init__(self, intents, messages):
        super().__init__(intents=intents)
        self.messages = messages
        self.__log = get_logger(name=self.__class__.__name__)

    async def on_ready(self):
        self.__log.info(f'{self.user} has connected to Discord!')

    async def on_message(self, message):
        if message.author == self.user:
            return

        self.__log.info(f"Message from {message.author}: {message.content}")
        self.messages.append(message.content)

if __name__ == "__main__":
    messages = []
    intents = discord.Intents.all()
    client = Jigglypuff(intents=intents, messages=messages)
    producer = DiscordProducer(client=client, messages=messages)

    client_task = threading.Thread(target=client.run, args=(TOKEN,))
    client_task.start()

    loop = asyncio.get_event_loop()
    producer_task = loop.run_until_complete(producer.run())

    

    