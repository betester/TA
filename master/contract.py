from abc import ABC, abstractmethod
import asyncio
from asyncio.locks import Lock
from collections.abc import Callable, Coroutine
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

class Master(ABC):

    @abstractmethod
    def add_new_consumer(self, topic_id, group_id):
        pass

class MachineConditionData(BaseModel):
    target_topic: str
    timestamp: int
    total_messages: int

class TopicDeploymentConfig(BaseModel):
    max_instance: int
    topic_id: str
    project_name: str
    service_name: str
    image_name: str
    zone: str
    service_account: str
    image_env: dict 
    machine_type: str
    provider: str

class InputOutputThroughputPair(BaseModel):
    source_topic: str
    target_topic: str
    deploy_configs: TopicDeploymentConfig 

class TopicDeployDelay(BaseModel):
    model_config = ConfigDict(ignored_types=(asyncio.Lock, ))
    can_be_deployed: bool
    deployed_timestamp: datetime
    _lock: asyncio.Lock = Lock()

class DeployArgs(BaseModel):
    source_topic: str
    source_topic_throughput: float
    target_topic: str
    target_topic_throughput: float

class DeployResult(BaseModel):
    machine_id: str
    shut_down_machine: Callable[[], Coroutine[Any, Any, bool]] 

class MasterObserver(ABC):

    @abstractmethod
    def on_receive(self, data: InputOutputThroughputPair | MachineConditionData):
        pass

    @abstractmethod
    async def start(self):
        pass

    @abstractmethod
    async def stop(self):
        pass

class TopicStatistic(ABC):

    @abstractmethod
    def get_topic_mean(self, topic: str) -> float:
        pass 

    @abstractmethod
    def get_topic_standard_deviation(self, topic: str) -> float:
        pass

