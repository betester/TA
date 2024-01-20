from aiokafka.client import asyncio
from fogverse import Manager, Consumer, Producer

class ProducerBoongan(Producer):
    def __init__(self, consumer: Consumer):
        self.consumer: Consumer = consumer
        self.message = None
        Producer.__init__(
            self, 
            producer_servers="localhost:9093", 
            producer_topic="testing",
        )
    
    async def _after_start(self):
        await asyncio.sleep(3)
        await self.send(b'Hello World')
        print("Message sent!")

    async def receive(self):
        await asyncio.sleep(2)
        return await self.consumer.receive()
    
    def _after_receive(self, message):
        print("Message received!")
        print(message)

class ConsumerBoongan(Consumer):
    def __init__(self):
        Consumer.__init__(
            self, 
            consumer_servers=["localhost:9093"], 
            consumer_topic=["testing"],
            group_id="testing"
        )

    async def _send(self, data, *args, **kwargs) -> asyncio.Future:
        print(data, args, kwargs)
        return data

async def main():
    manager = Manager()
    consumer = ConsumerBoongan()
    producer = ProducerBoongan(consumer)
    components = [consumer, producer]
    await manager.run_components(components)


if __name__ == '__main__':
    asyncio.run(main())
