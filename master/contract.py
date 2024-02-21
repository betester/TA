
from abc import ABC, abstractmethod

from pydantic import BaseModel


class Master(ABC):

    @abstractmethod
    def add_new_consumer(self, topic_id, group_id):
        pass

class InputOutputThroughputPair(BaseModel):
    source_topic: str
    target_topic: str

class MachineConditionData(BaseModel):
    target_topic: str
    timestamp: int
