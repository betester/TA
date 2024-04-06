import discord
from fogverse.fogverse_logging import get_logger
import asyncio
import os
import csv

class DiscordClient(discord.Client):
    def __init__(self, intents, messages, producer):
        super().__init__(intents=intents)
        self.messages = messages
        self.__log = get_logger(name=self.__class__.__name__)
        self.producer = producer
        self.delay = None

    async def on_ready(self):
        self.__log.info(f'{self.user} has connected to Discord!')

    async def on_message(self, message):
        if message.author == self.user:
            return
        
        if message.content.startswith('$start'):
            # usage $start <delay>
            try:
                delay = message.content.split(" ")[1]
                self.delay = float(delay)
                self.loop.create_task(
                    self.mock_send()
                )
            except:
                self.delay = None

        elif message.content == '$stop':
            # usage $stop
            self.delay = None

        else:
            self.__log.info(f"Message from {message.author}: {message.content}")
            await self.messages.put(message.content)

    async def setup_hook(self):
        self.loop.create_task(
            self.producer.run()
        )
        
    async def mock_send(self):
        csv_paths = [os.path.join('./data/crawler/kaggle', file) for file in os.listdir('./data/crawler/kaggle') if file.endswith('.csv')]
        for csv_path in csv_paths:
            with open(csv_path, 'r') as csv_file:
                res = csv.reader(csv_file)
                for row in res:
                    await asyncio.sleep(self.delay)
                    await self.messages.put(row[3])
        




