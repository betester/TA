
from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Optional

from pydantic import BaseModel


class Master(ABC):

    @abstractmethod
    def add_new_consumer(self, topic_id, group_id):
        pass

class MachineConditionData(BaseModel):
    target_topic: str
    timestamp: int
    total_messages: int
    
class CloudProvider(StrEnum):
    GOOGLE_CLOUD = "GOOGLE_CLOUD" 
    AWS = "AWS"

class CloudDeployConfigs:
    provider: CloudProvider = CloudProvider.GOOGLE_CLOUD
    zone: str
    env: dict[str, str]

class TopicDeploymentConfig:
    topic_id: str
    service_name: str
    cloud_deploy_configs: CloudDeployConfigs

#TODO: rename this into something else
class InputOutputThroughputPair(BaseModel):
    source_topic: str
    target_topic: str
    deploy_configs: Optional[TopicDeploymentConfig] = None

class MasterObserver(ABC):

    @abstractmethod
    def on_receive(self, data: InputOutputThroughputPair | MachineConditionData):
        pass

    @abstractmethod
    async def start(self):
        pass

class TopicStatistic(ABC):

    @abstractmethod
    def get_topic_mean(self, topic: str) -> float:
        pass 


    @abstractmethod
    def get_topic_standard_deviation(self, topic: str) -> float:
        pass

