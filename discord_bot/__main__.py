import discord
import asyncio
from discord_bot.producer import DiscordProducer
from discord_bot.bot import DiscordClient
from master.component import MasterComponent
import os

TOKEN = str(os.getenv("TOKEN", ""))

async def main():
    '''
    For some reason, if the producer is not in the same event loop as the discord client, the producer will not work.
    Inject the producer into the client so the producer is on the same event loop,
    For syncing messages accross the two, use a queue.
    '''
    messages = asyncio.Queue()
    observer = MasterComponent().producer_observer()

    intents = discord.Intents.all()
    discord_producer = DiscordProducer(messages, observer)
    discord_client = DiscordClient(intents, messages, discord_producer)
    

    await discord_client.start(TOKEN)

if __name__ == "__main__":
    asyncio.run(main())