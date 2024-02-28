import discord
from fogverse.fogverse_logging import get_logger
from fogverse import Producer, Profiling
from crawler import CrawlerResponse
import asyncio
import threading

TOKEN = 'Forgor again :skull:'
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
    

class Jigglypuff(discord.Client):
    def __init__(self, intents, messages, producer):
        super().__init__(intents=intents)
        self.messages = messages
        self.__log = get_logger(name=self.__class__.__name__)
        self.producer = producer

    async def on_ready(self):
        self.__log.info(f'{self.user} has connected to Discord!')

    async def on_message(self, message):
        if message.author == self.user:
            return

        self.__log.info(f"Message from {message.author}: {message.content}")
        await self.messages.put(message.content)


    async def setup_hook(self):
        self.loop.create_task(
            self.producer.run()
        )


async def main():

    messages = asyncio.Queue()
    intents = discord.Intents.all()

    # run both the client and the producer without blocking

    discord_producer = DiscordProducer(messages)
    jigglypuff = Jigglypuff(intents, messages, discord_producer)

    await jigglypuff.start(TOKEN)



if __name__ == "__main__":
    asyncio.run(main())
