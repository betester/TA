
from aiokafka.client import asyncio
from confluent_kafka.admin import (
    AdminClient,
    ConsumerGroupDescription,
    TopicDescription,
    NewPartitions
)
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from fogverse.fogverse_logging import get_logger
from confluent_kafka import TopicCollection
from fogverse.util import get_timestamp
from master.contract import InputOutputThroughputPair, MachineConditionData

class ConsumerAutoScaler:

    lock = asyncio.Lock()
    
    def __init__(self, kafka_admin: AdminClient, sleep_time: int):
        self._kafka_admin = kafka_admin
        self._logger = get_logger(name=self.__class__.__name__)
        self._sleep_time = sleep_time
    

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


    async def _start(self, *args, **kwargs):

        
        async with ConsumerAutoScaler.lock: 

            partition_is_enough = False

            consumer_group: str = kwargs.pop("consumer_group", None)
            consumer_topic: str = kwargs.pop("consumer_topic", None)
            consumer = kwargs.pop("consumer", None)
            producer = kwargs.pop("producer", None)

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
                self._logger.info("No partition assigned for retrying")
                consumer.unsubscribe()
                consumer.subscribe([consumer_topic])
                await asyncio.sleep(self._sleep_time)


            self._logger.info("Successfully assigned, consuming")
            await consumer.seek_to_end()

class ProducerObserver:

    def __init__(self, producer_topic: str):
        self._producer_topic = producer_topic 


    async def send_input_output_ratio_pair(self, source_topic: str, target_topic: str, producer: AIOKafkaProducer):
        '''
        Identify which topic pair should the observer ratio with
        '''
        if source_topic is not None:
            await producer.send(
                topic=self._producer_topic,
                value=self._input_output_pair_data_format(source_topic, target_topic)
            )
    
    async def send_success_send_timestamp(self, target_topic: str, producer: AIOKafkaProducer):
        if target_topic is not None:

            data = self._success_timestamp_data_format(target_topic)
            await producer.send(
                topic=self._producer_topic,
                value=data
            )

    def _input_output_pair_data_format(self, source_topic, target_topic: str):
        return InputOutputThroughputPair(
            source_topic=source_topic,
            target_topic=target_topic
        ).model_dump_json().encode()
        
    def _success_timestamp_data_format(self, target_topic: str):
        return MachineConditionData(
            target_topic=target_topic,
            timestamp=int(get_timestamp().timestamp())
        ).model_dump_json().encode()
