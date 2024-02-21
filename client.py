
from aiokafka.client import asyncio
from fogverse import Consumer, Profiling
from master.master import ConsumerAutoScaler
from confluent_kafka.admin import AdminClient


class Client(Profiling, Consumer):
        
    def __init__(self, number):
        self.consumer_topic =  "client_v6"
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


if __name__ == "__main__":
    client = Client(1)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(client.run())

