from aiokafka.client import asyncio
from fogverse import Consumer
from fogverse.fogverse_logging import get_logger
from master.contract import InputOutputThroughputPair, MachineConditionData, MasterObserver


class Master(Consumer):

    def __init__(self, 
                 consumer_topic: str, 
                 consumer_servers: str,
                 consumer_group_id: str,
                 observers: list[MasterObserver]):

        self.consumer_topic =  consumer_topic
        self.consumer_servers = consumer_servers
        self.group_id = consumer_group_id
        self._log = get_logger(name=self.__class__.__name__)

        self.auto_decode = False

        self._closed = False
        self._observers = observers
        Consumer.__init__(self)
        
    def decode(self, data: bytes) -> InputOutputThroughputPair | MachineConditionData:
        try:
            return InputOutputThroughputPair.model_validate_json(data, strict=True)
        except Exception:
            return MachineConditionData.model_validate_json(data, strict=True)
    
    
    async def process(self, data: InputOutputThroughputPair | MachineConditionData):
        for observer in self._observers:
            observer.on_receive(data)

    def encode(self, data):
        pass

    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        pass

    async def _start(self):
        observer_tasks = [asyncio.create_task(observer.start()) for observer in self._observers]
        consumer_task = asyncio.create_task(super()._start())
        print("Starting master")
        await asyncio.gather(*observer_tasks, consumer_task)

    async def close_consumer(self):

        for observer in self._observers:
            await observer.stop()

        await super().close_consumer()


