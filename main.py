from aiokafka.client import asyncio
from fogverse import Manager, Consumer, Producer

class ProducerBoongan(Producer):
    def __init__(self):
        Producer.__init__(
            self, 
            producer_servers="localhost:9092", 
            producer_topic="testing",
        )

class ConsumerBoongan(Consumer):
    def __init__(self):
        Consumer.__init__(
            self, 
            consumer_servers=["localhost:9092"], 
            group_id="testing"
        )

async def main():
    manager = Manager()
    consumer ,producer = ConsumerBoongan(), ProducerBoongan()
    components = [consumer, producer]
    await manager.run_components(components)


if __name__ == '__main__':
    asyncio.run(main())
