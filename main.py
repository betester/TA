from fogverse import Consumer, Producer
from uuid import uuid4 
import asyncio


class MyConsumer(Consumer):
    
    def __init__(self):
        self.consumer_topic = ['test']
        self.consumer_servers = ["localhost:9092","localhost:9093","localhost:9094"]
        self._consumer_conf = {
            'group_id': str(uuid4())
        }
        Consumer.__init__(self)

class MyProducer(Producer):

    def __init__(self):
        self.producer_topic = "test"
        self.consumer_servers = ["localhost:9092","localhost:9093","localhost:9094"]
        Producer.__init__(self)

async def main():
    tasks  = [MyConsumer().run(), MyProducer().run()]
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        print(e)
        for t in tasks:
            t.close()


if __name__ == '__main__':
    asyncio.run(main())