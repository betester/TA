
from aiokafka.client import asyncio
from confluent_kafka.admin import (
    AdminClient,
    ConsumerGroupDescription,
    TopicDescription,
    NewPartitions
)
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from fogverse.fogverse_logging import get_logger
from fogverse import AbstractConsumer
from confluent_kafka import  TopicCollection

class AutoScalingConsumer:

    lock = asyncio.Lock()
    
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
        topic_future_description = self._kafka_admin.describe_topics(TopicCollection([topic_id]))[topic_id]
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

    async def _start(self, *args, **kwargs):
        
        async with AutoScalingConsumer.lock: 

            partition_is_enough = False

            consumer_group: str = str(getattr(self, 'group_id', None))
            consumer_topic: str = str(getattr(self, 'consumer_topic', None))
            consumer = getattr(self, 'consumer') 
            producer = getattr(self, 'producer')
            client_id = getattr(self, 'number')

            consumer_exist = isinstance(consumer, AIOKafkaConsumer)
            producer_exist = isinstance(producer, AIOKafkaProducer)

            if not consumer_exist:
                raise Exception("Consumer does not exist")

            if producer_exist:
                self._logger.info("Starting producer")
                await producer.start()

            # initial start
            self._logger.info("Starting consumer")
            await consumer.start()
            while not partition_is_enough:
                try:
                    group_id_total_consumer = await self._group_id_total_consumer(consumer_group) 
                    topic_id_total_partition = await self._topic_id_total_partition(consumer_topic)

                    self._logger.info(f"\ngroup_id_total_consumer: {group_id_total_consumer}\ntopic_id_total_partition:{topic_id_total_partition}")

                    partition_is_enough = topic_id_total_partition >= group_id_total_consumer

                    if not partition_is_enough:
                        self._logger.info(f"Adding {group_id_total_consumer} partition to topic {consumer_topic}")
                        self._add_partition_on_topic(consumer_topic, group_id_total_consumer)

                except Exception as e:
                    self._logger.error(e)

                    
                await asyncio.sleep(self._sleep_time)
            

            while len(consumer.assignment()) == 0:
                self._logger.info(f"No partition assigned for client {client_id}, retrying")
                consumer.unsubscribe()
                consumer.subscribe([consumer_topic])
                await asyncio.sleep(self._sleep_time)

            await consumer.seek_to_end()

