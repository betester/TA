from fogverse import Consumer, Producer
from uuid import uuid4 
import asyncio


class MyConsumer(Consumer):
    
    def __init__(self):
        self.consumer_topic = ['example-topic']
        self.consumer_servers = ["localhost:9092"]
        self.always_read_last = False
        self.consumer_conf = {'group_id': str(uuid4())}
        Consumer.__init__(self)
    
class MyProducer(Producer):

    def __init__(self, consumer: MyConsumer):
        self.producer_topic = "example-topic"
        self.consumer: MyConsumer = consumer
        self.producer_servers = ["localhost:9092"]
        self.message = {
            "key": None
        }
        Producer.__init__(self)

    async def receive(self):
        return self.consumer.receive()
        
    

    async def _after_receive(self, message):
        print(message)
        print("GAK MASUK AKAL")

async def main():
    consumer = MyConsumer()
    producer = MyProducer(consumer)
    tasks  = [consumer.run(), producer.run()]
    await producer.send("Hello")
    print("Finish sending")
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        print(e)
        for t in tasks:
            t.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()