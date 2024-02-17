
from aiokafka.client import asyncio
from confluent_kafka.admin import (
    AdminClient,
    ConsumerGroupDescription,
    TopicDescription,
    NewPartitions
)

from fogverse.fogverse_logging import get_logger
from fogverse import AbstractConsumer

class AutoScalingConsumer:
    
    def __init__(self, kafka_admin: AdminClient, sleep_time: int):
        self._kafka_admin = kafka_admin
        self._logger = get_logger(name=self.__class__.__name__)
        self._sleep_time = sleep_time
        
        # making sure that the class is consumer class
        self.is_consumer_class()
    

    async def _group_id_total_consumer(self, group_id: str) -> int:
        group_future_description = self._kafka_admin.describe_consumer_groups([group_id])[group_id]
        group_description: ConsumerGroupDescription = await asyncio.wrap_future(group_future_description) 
        return len(group_description.members)
    
    async def _topic_id_total_partition(self, topic_id: str) -> int:
        topic_future_description = self._kafka_admin.describe_topics([topic_id])[topic_id]
        topic_description: TopicDescription = await asyncio.wrap_future(topic_future_description) 
        return len(topic_description.partitions)
    

    def _add_partition_on_topic(self, topic_id: str, new_total_partition: int):
        future_partition = self._kafka_admin.create_partitions([NewPartitions(topic_id, new_total_partition)])[topic_id]

        # waits until the partition is created
        future_partition.result()


    @classmethod
    def is_consumer_class(cls):
        if not issubclass(cls, AbstractConsumer):
            raise Exception("This class should be inherited by consumer")

    async def _before_start(self):
        
        partition_is_enough = False

        consumer_group: str = str(getattr(self, 'group_id', None))
        consumer_topic: str = str(getattr(self, 'consumer_topic', None))

        loop = asyncio.get_event_loop()

        while not partition_is_enough:
            try:
                group_id_total_task = loop.create_task(self._group_id_total_consumer(consumer_group)) 
                topic_id_total_partition_task = loop.create_task(self._topic_id_total_partition(consumer_topic))

                group_id_total_consumer, topic_id_total_partition = asyncio.gather(group_id_total_task, topic_id_total_partition_task)

                partition_is_enough = topic_id_total_partition > group_id_total_consumer

                if not partition_is_enough:
                    self._add_partition_on_topic(consumer_topic, group_id_total_consumer)
            except Exception as e:
                self._logger.error(e)

            await asyncio.sleep(self._sleep_time)
