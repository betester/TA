import discord
from fogverse.fogverse_logging import get_logger

class DiscordClient(discord.Client):
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



