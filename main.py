from aiokafka.client import asyncio
from component import KaggleCrawlerComponent
from fogverse import Consumer
from analyzer import TestAnalyzer

class Client(Consumer):
        
    def __init__(self):
        self.consumer_topic =  "client"
        self.consumer_servers = "localhost:9092"
        Consumer.__init__(self)
    
    async def process(self, data):
        print("#" *10 +  "CLIENT" + "#" * 10)
        print(data)
        return data
    
    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        return data



async def main():
    
    # components 
    kaggle_crawler_component = KaggleCrawlerComponent(
        directory_path="./data/crawler/kaggle"
    )
    
    # producers or consumers
    client = Client()
    analyzer = TestAnalyzer()
    kaggle_crawler_producer = kaggle_crawler_component.crawler_producer

    return await asyncio.gather(
        client.run(),
        analyzer.run(),
        kaggle_crawler_producer.run()
    )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
