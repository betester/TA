from aiokafka.client import asyncio
from fogverse import Consumer, Profiling, get_timestamp
from fogverse.fogverse_logging import get_logger
import json

@DeprecationWarning
class Observer(Consumer, Profiling):
    """
    Experimental class to observe the messages from the client_v6 and analyze topics
    """

    def __init__(self):
        self.consumer_topic =  ["client_v6", "analyze"]
        self.consumer_servers = "localhost:9092"
        self.group_id = "observer"
        self.counter = {'raw': 0, 'processed': 0}
        self.input_timestamps = []
        self._closed = False
        self.__log = get_logger(name=self.__class__.__name__)
        Consumer.__init__(self)
        Profiling.__init__(self, name='observer-logs', dirname='observer-logs')

    def _after_receive(self, data):
        result = super()._after_receive(data)

        if len(self.input_timestamps) < 2:
            return 

        old, new = self.input_timestamps[0], self.input_timestamps.pop(-1)

        diff = new - old

        if diff.total_seconds() >= 60:
            self.__log.info(f"Total raw messages: {self.counter['raw']}, Ratio: {self.counter['raw']/60}")
            self.__log.info(f"Total processed messages: {self.counter['processed']}, Ratio: {self.counter['processed']/60}")
            self.input_timestamps.pop()
            self.counter = {'raw': 0, 'processed': 0}

        return result

    async def receive(self):
        result = await super().receive()
        self.input_timestamps.append(get_timestamp(utc=False))
        return result
    
    async def process(self, data):
        # print(data)
        dict_data = json.loads(data)
        
        try:
            dict_data['is_disaster']
            self.counter['processed'] += 1
        except KeyError:
            self.counter['raw'] += 1

        return data
    
    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        return data

if __name__ == "__main__":
    client = Observer()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(client.run())

