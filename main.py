from aiokafka.client import asyncio
from component import CrawlerComponent
from fogverse import Consumer
from analyzer import TestAnalyzer

class Client(Consumer):
        
    def __init__(self):
        self.consumer_topic =  "client"
        self.consumer_servers = "localhost:9092"
        self._closed = False
        Consumer.__init__(self)
    
    async def process(self, data):
        print("#" *10 +  "CLIENT" + "#" * 10)
        print(data)
        return data
    
    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        return data



async def main():

    # loop
    loop = asyncio.get_event_loop()
    
    # components 
    kaggle_crawler_component = CrawlerComponent()
    
    # producers or consumers
    client_task = loop.create_task(Client().run())
    analyzer_task = loop.create_task(TestAnalyzer().run()) 

    kaggle_crawler_producer = kaggle_crawler_component.crawler_producer(
        directory_path="./data/crawler/kaggle"
    )

    kaggle_crawler_task = loop.create_task(kaggle_crawler_producer.run())

    return await asyncio.gather(
        client_task,
        analyzer_task,
        kaggle_crawler_task,
    )


if __name__ == "__main__":
    asyncio.run(main())
