from fogverse import Consumer, Producer
import asyncio


class MyConsumer(Consumer):
  
    def __init__(self):
        self.consumer_topic = ['example-topic']
        self.consumer_servers = "localhost:9092" 
        Consumer.__init__(self)
    
    def _before_start(self):
        print("Starting consumer")

    def _after_start(self):
        print("Consumer Started")
    
    async def _send(self, data, *args, **kwargs) -> asyncio.Future:
        print(data, args, kwargs)
        return data

    
    
class MyProducer(Producer):

    def __init__(self, consumer: Consumer):
        self.producer_topic = "example-topic"
        self.producer_servers = ["localhost:9092"]
        self.message = None
        self.consumer: Consumer = consumer
        Producer.__init__(self)

    async def _after_start(self):
        await asyncio.sleep(3)
        await self.send(b'Hello World')
        print("Message sent!")

    def _before_receive(self):
        print("initiating receive")
    
    def _after_receive(self, message):
        print("Message received!")
        print(message)
    
    async def receive(self):
        return await self.consumer.receive() 
    
    def callback(self, record_metadata, *args, **kwargs):
        print(record_metadata, args, kwargs)

async def main():
    consumer = MyConsumer()
    producer = MyProducer(consumer)
    tasks = [ consumer.run(), producer.run()]
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
