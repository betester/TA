from aiokafka.client import asyncio
from fogverse import Consumer
from fogverse.fogverse_logging import get_logger
from master.contract import InputOutputThroughputPair, MachineConditionData


class Master(Consumer):

    def __init__(self, 
                 consumer_topic: str, 
                 consumer_servers: str,
                 consumer_group_id: str,
                 scheduler_time: int):

        self.consumer_topic =  consumer_topic
        self.consumer_servers = consumer_servers
        self.group_id = consumer_group_id
        self.__log = get_logger(name=self.__class__.__name__)
        self.auto_decode = False

        self._closed = False

        Consumer.__init__(self)

        self.ratio_pair: dict[str, list[str]] = {}
        self.topic_timestamps: dict[str, int] = {}
        
        self._scheduler_time = scheduler_time

        self._schedule_task = asyncio.create_task(self._check_topics_throughput_ratio())
        
    def decode(self, data: bytes) -> InputOutputThroughputPair | MachineConditionData:
        try:
            return InputOutputThroughputPair.model_validate_json(data, strict=True)
        except Exception:
            return MachineConditionData.model_validate_json(data, strict=True)
    
    
    async def process(self, data: InputOutputThroughputPair | MachineConditionData):
        if isinstance(data, InputOutputThroughputPair):
            source_topic, target_topic = data.source_topic, data.target_topic
            topics = self.ratio_pair.get(source_topic, [])
            topics.append(target_topic)
            self.ratio_pair[source_topic] = topics
            self.__log.info(f"Received source topic pair: {source_topic} and {target_topic}")

        elif isinstance(data, MachineConditionData): 
            source_topic = data.target_topic
            timestamps = self.topic_timestamps.get(source_topic, 0)
            timestamps += 1
            self.topic_timestamps[source_topic] = timestamps


    def encode(self, data):
        pass

    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        pass
    
    async def _check_topics_throughput_ratio(self):

        while True:
            await asyncio.sleep(self._scheduler_time)
            for source_topic, target_topics in self.ratio_pair.items():
                source_topic_throughput = self.topic_timestamps.get(source_topic, 0)
                for target_topic in target_topics:
                    target_topic_throughput = self.topic_timestamps.get(target_topic, 0)
                    throughput_ratio = target_topic_throughput/max(source_topic_throughput, 1)
                    if throughput_ratio == target_topic_throughput:
                        self.__log.error(f"Source topic {source_topic} throughput is {source_topic_throughput}, the machine might be dead")
                    self.__log.info(f"Source topic: {source_topic} ratio with {target_topic}: {throughput_ratio}")
    

            for topic, throughput in self.topic_timestamps.items():
                self.__log.info(f"Topic {topic} total message in {self._scheduler_time} seconds: {throughput}")
                self.topic_timestamps[topic] = 0
