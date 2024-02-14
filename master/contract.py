
from abc import ABC, abstractmethod

from pydantic import BaseModel

class AddConsumerRequest(BaseModel):
    topic_id: str
    group_id: str

class AddConsumerResponse(BaseModel):
    consumer_can_consume: bool

class ConsumerMaster(ABC):

    @abstractmethod
    def add_consumer(self, request: AddConsumerRequest) -> bool:
        '''
        returns boolean value which indicates whether partition number is enough or not
        if it's false then the consumer would not be able to consume the topic
        '''
        pass
