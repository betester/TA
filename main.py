from aiokafka.client import asyncio
from fogverse import Consumer
from analyzer import TestAnalyzer

from crawler.crawler import TestCrawler


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
    test_crawler = TestCrawler()
    test_analyzer = TestAnalyzer()
    client = Client()
    return await asyncio.gather(test_crawler.run(), test_analyzer.run(), client.run())


if __name__ == "__main__":
    asyncio.run(main())
