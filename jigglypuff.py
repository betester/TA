import discord
from fogverse.fogverse_logging import get_logger
from fogverse import Producer, Profiling
from crawler import CrawlerResponse
import asyncio
import threading

TOKEN = 'ODg3MjUwODg5ODc2ODYwOTQ4.GioG3m.08ND3O-n1l2EIxGcIZZS8Rq8UvYAvJHqlIkGLM'
PRODUCER_TOPIC = "analyze_v1"
PRODUCER_SERVERS = "localhost:9092"
CONSUMER_GROUP_ID = "crawler"

class DiscordProducer(Producer, Profiling):
    def __init__(self, messages):
        self.producer_topic = PRODUCER_TOPIC 
        self.producer_servers = PRODUCER_SERVERS
        self.group_id = CONSUMER_GROUP_ID
        self.messages = messages
        self._closed = False
        Producer.__init__(self)
        Profiling.__init__(self, name='discord-logs', dirname='discord-logs')

    async def receive(self):
        await asyncio.sleep(0.01)
        try:
            data = self.messages.get_nowait()
            return CrawlerResponse(
                message=data,
                source="discord"
            )
        
        except asyncio.QueueEmpty:
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
        self.messages.put_nowait(message.content)

if __name__ == "__main__":
    messages = asyncio.Queue()
    intents = discord.Intents.all()

    # run both the client and the producer without blocking
    jigglypuff = Jigglypuff(intents, messages)
    discord_producer = DiscordProducer(messages)

    threading.Thread(target=jigglypuff.run, args=(TOKEN,), daemon=True).start()

    loop = asyncio.get_event_loop()
    producer_task = loop.create_task(discord_producer.run())
    threading.Thread(target=loop.run_until_complete, args=(producer_task,), daemon=True).start()

    while True:
        pass