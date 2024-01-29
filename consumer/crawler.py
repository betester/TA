from asyncio import Queue


class CrawlerConsumer:

    def __init__(self, consumer_num: int, queue: Queue):
        self.consumer_num = consumer_num
        self.queue = queue
        self.stop = False

    async def consume(self):

        while not self.stop:
            value = await self.queue.get()
            print(f"{self.consumer_num}: consuming {value}")

    def stop_consuming(self):
        self.stop = True

