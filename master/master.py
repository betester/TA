
import time

from collections.abc import Callable
from typing import Any
from aiokafka.client import asyncio
from confluent_kafka.admin import (
    AdminClient,
    ConsumerGroupDescription,
    KafkaError,
    TopicDescription,
    NewPartitions,
    NewTopic
)
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from fogverse.fogverse_logging import get_logger
from confluent_kafka import  Consumer, KafkaException, TopicCollection
from fogverse.util import get_timestamp
from master.contract import InputOutputThroughputPair, MachineConditionData

class ConsumerAutoScaler:

    lock = asyncio.Lock()
    
    def __init__(self, kafka_admin: AdminClient, sleep_time: int, initial_total_partition: int=1):
        self._kafka_admin = kafka_admin
        self._logger = get_logger(name=self.__class__.__name__)
        self._sleep_time = sleep_time
        self._initial_total_partition = initial_total_partition
    

    def _group_id_total_consumer(self, group_id: str) -> int:
        group_future_description = self._kafka_admin.describe_consumer_groups([group_id])[group_id]
        group_description: ConsumerGroupDescription = group_future_description.result()
        return len(group_description.members)
    
    def _topic_id_total_partition(self, topic_id: str) -> int:
        topic_future_description = self._kafka_admin.describe_topics(TopicCollection([topic_id]))[topic_id]
        topic_description: TopicDescription = topic_future_description.result()
        return len(topic_description.partitions)
    

    def _add_partition_on_topic(self, topic_id: str, new_total_partition: int):
        future_partition = self._kafka_admin.create_partitions([NewPartitions(topic_id, new_total_partition)])[topic_id]

        # waits until the partition is created
        future_partition.result()

    def _topic_exist(self, topic_id: str, retry_count: int) -> bool:
        total_retry = 0

        while total_retry <= retry_count:
            try:
                topic_future_description = self._kafka_admin.describe_topics(TopicCollection([topic_id]))[topic_id]
                topic_future_description.result()
                return True
            except KafkaException as e:
                error = e.exception.args[0]
                if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART or not error.retriable():
                    return False
                self._logger.error(e)
                self._logger.info("Retrying to check if topic exist")

            total_retry += 1
            time.sleep(self._sleep_time)

        return False

    def _create_topic(self, topic_id: str, retry_count: int) -> bool:
        total_retry = 0

        while total_retry <= retry_count:
            try:
                create_topic_future = self._kafka_admin.create_topics([
                    NewTopic(
                        topic_id,
                        num_partitions=self._initial_total_partition
                    )
                ])[topic_id]
                create_topic_future.result()
                self._logger.info(f"{topic_id} is created")
                return True

            except KafkaException as e:
                error = e.exception.args[0]
                
                if error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    self._logger.info(f"{topic_id} already created")
                    return True

                self._logger.error(e)
                if not error.retriable():
                    return False
                self._logger.info(f"Retrying creating topic {topic_id}")
                total_retry += 1
                time.sleep(self._sleep_time)

        return False

    def start(self,
              consumer: Consumer,
              topic_id: str,
              group_id: str,
              retry_topic_creation: int=3):

        if not self._topic_exist(topic_id, retry_topic_creation):
            topic_created = self._create_topic(topic_id, retry_topic_creation)
            if not topic_created:
                raise Exception("Topic cannot be created")

        partition_is_enough = False
        consumer.subscribe([topic_id])

        while not partition_is_enough:
            try:
                group_id_total_consumer = self._group_id_total_consumer(group_id) 
                topic_id_total_partition = self._topic_id_total_partition(topic_id)

                self._logger.info(f"\ngroup_id_total_consumer: {group_id_total_consumer}\ntopic_id_total_partition:{topic_id_total_partition}")

                partition_is_enough = topic_id_total_partition >= group_id_total_consumer

                if not partition_is_enough:
                    self._logger.info(f"Adding {group_id_total_consumer} partition to topic {topic_id}")
                    self._add_partition_on_topic(topic_id, group_id_total_consumer)

            except Exception as e:
                self._logger.error(e)
                
            time.sleep(self._sleep_time)

        while len(consumer.assignment()) == 0:
            self._logger.info("No partition assigned for retrying")
            consumer.unsubscribe()
            consumer.subscribe([topic_id])

        self._logger.info("Successfully assigned, consuming")




    async def async_start(self, *args, **kwargs):

        
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
                    group_id_total_consumer = self._group_id_total_consumer(consumer_group) 
                    topic_id_total_partition = self._topic_id_total_partition(consumer_topic)

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


    def send_input_output_ratio_pair(self, source_topic: str, target_topic: str, send: Callable[[str, bytes], Any]):
        '''
        Identify which topic pair should the observer ratio with
        send: a produce function from kafka
        '''
        if source_topic is not None:
            return send(
                self._producer_topic,
                self._input_output_pair_data_format(source_topic, target_topic)
            )
    
    def send_total_successful_messages(self, target_topic: str, total_messages: int, send: Callable[[str, bytes], Any]):
        if target_topic is not None:
            data = self._success_timestamp_data_format(target_topic, total_messages)
            return send(
                self._producer_topic,
                data
            )

    def _input_output_pair_data_format(self, source_topic, target_topic: str):
        return InputOutputThroughputPair(
            source_topic=source_topic,
            target_topic=target_topic
        ).model_dump_json().encode()
        
    def _success_timestamp_data_format(self, target_topic: str, total_messages: int):
        return MachineConditionData(
            target_topic=target_topic,
            total_messages=total_messages,
            timestamp=int(get_timestamp().timestamp())
        ).model_dump_json().encode()
