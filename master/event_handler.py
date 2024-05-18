from aiokafka.client import asyncio
from pydantic import BaseModel
from fogverse import Consumer
from fogverse.fogverse_logging import get_logger
from master.contract import (
    ConsumerAssignedPartitionEvent,
    ConsumerRemovedPartitionEvent,
    InputOutputThroughputPair,
    MachineConditionData,
    MasterObserver,
    ProfillingExpectedConsumer
)


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

        self.possible_data_types : list[type[BaseModel]] = [
            InputOutputThroughputPair,
            MachineConditionData,
            ConsumerRemovedPartitionEvent,
            ConsumerAssignedPartitionEvent,
            ProfillingExpectedConsumer
        ]

        self.auto_decode = False

        self._closed = False
        self._observers = observers
        Consumer.__init__(self)
        
    def decode(self, data: bytes):
        for data_types in self.possible_data_types:
            try:
                return data_types.model_validate_json(data, strict=True)
            except:
                continue
        self._log.error(f"Not a valid request {data}")
        return None

    
    async def process(self, data: BaseModel):
        for observer in self._observers:
            observer.on_receive(data)

    def encode(self, data):
        pass

    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        pass

    async def _start(self):
        [asyncio.create_task(observer.start()) for observer in self._observers]
        await super()._start()

    async def close_consumer(self):

        for observer in self._observers:
            await observer.stop()

        await super().close_consumer()
