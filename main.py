from aiokafka.client import asyncio
from analyzer.component import AnalyzerComponent
from component import Component
from fogverse import Consumer, Profiling

class Client(Profiling, Consumer):
        
    def __init__(self, number):
        self.consumer_topic =  "client"
        self.consumer_servers = "localhost:9092"
        self.group_id = "client"
        self.number = number
        self._closed = False
        Consumer.__init__(self)
        Profiling.__init__(self, name='client-logs', dirname='client-logs')
    
    async def process(self, data):
        print("#" *10 +  f"CLIENT {self.number}" + "#" * 10)
        print(data)
        return data
    
    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        return data



async def main():

    # loop
    loop = asyncio.get_event_loop()
    
    # components 
    component = Component()
    analyzer_component = AnalyzerComponent()
    
    # producers or consumers
    client_task = loop.create_task(Client(1).run())
    client_task_2 = loop.create_task(Client(2).run())
    kaggle_crawler_producer = component.crawler_producer(
        directory_path="./data/crawler/kaggle"
    )
    analyzer_producer = analyzer_component.disaster_analyzer()

    analyzer_task = loop.create_task(analyzer_producer.run()) 
    kaggle_crawler_task = loop.create_task(kaggle_crawler_producer.run())

    return await asyncio.gather(
        client_task,
        client_task_2,
        analyzer_task,
        kaggle_crawler_task,
        
    )


if __name__ == "__main__":
    asyncio.run(main())
