import time

from collections.abc import Callable, Coroutine
from typing import Any, Tuple
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

from confluent_kafka import Consumer, KafkaException, TopicCollection
from fogverse.util import get_timestamp
from master.contract import (
    CloudProvider,
    DeployResult, 
    InputOutputThroughputPair, 
    MachineConditionData, 
    MasterObserver,
    TopicDeployDelay, 
    TopicDeploymentConfig, 
    TopicStatistic
)

class ConsumerAutoScaler:

    lock = asyncio.Lock()
    OFFSET_OUT_OF_RANGE = -1001 
    
    def __init__(self, kafka_admin: AdminClient, sleep_time: int, initial_total_partition: int=1):
        self._kafka_admin = kafka_admin
        self._sleep_time = sleep_time
        self._initial_total_partition = initial_total_partition
        self.consumer_is_assigned = False
    

    def _group_id_total_consumer(self, group_id: str) -> int:
        group_future_description = self._kafka_admin.describe_consumer_groups([group_id])[group_id]
        group_description: ConsumerGroupDescription = group_future_description.result()
        print(group_description.members)
        return len(group_description.members)
    
    def _topic_id_total_partition(self, topic_id: str) -> int:
        topic_future_description = self._kafka_admin.describe_topics(TopicCollection([topic_id]))[topic_id]
        topic_description: TopicDescription = topic_future_description.result()
        print(topic_description)
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
                error = e.args[0]
                if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART or not error.retriable():
                    return False
                print(e)
                print("Retrying to check if topic exist")

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
                print(f"{topic_id} is created")
                return True

            except KafkaException as e:
                error = e.exception.args[0]
                
                if error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    print(f"{topic_id} already created")
                    return True

                print(e)
                if not error.retriable():
                    return False
                print(f"Retrying creating topic {topic_id}")
                total_retry += 1
                time.sleep(self._sleep_time)

        return False

    def start(self,
              consumer: Consumer,
              topic_id: str,
              group_id: str,
              /,
              retry_attempt: int=3):

        print("Creating topic if not exist")
        self.topic_id = topic_id

        if not self._topic_exist(topic_id, retry_attempt):
            topic_created = self._create_topic(topic_id, retry_attempt)
            if not topic_created:
                raise Exception("Topic cannot be created")

        print(f"Subscribing to topic {topic_id}")
        consumer.subscribe(
            topics=[topic_id], 
            on_assign = self.on_consumer_assigned
        )

        while not self.consumer_is_assigned:
            try:
                group_id_total_consumer = self._group_id_total_consumer(group_id) 
                topic_id_total_partition = self._topic_id_total_partition(topic_id)

                print(f"\ngroup_id_total_consumer: {group_id_total_consumer}\ntopic_id_total_partition:{topic_id_total_partition}")

                partition_is_enough = topic_id_total_partition >= group_id_total_consumer

                if not partition_is_enough:
                    print(f"Adding {group_id_total_consumer} partition to topic {topic_id}")
                    self._add_partition_on_topic(topic_id, group_id_total_consumer)

                consumer.poll(self._sleep_time)
            except Exception as e:
                print(e)

    def on_consumer_assigned(self, consumer, partitions):

        try:
            if self.consumer_is_assigned:
                return

            committed_messages = consumer.committed(partitions)
            if len(committed_messages) == 0:
                print("Failed to be assigned, retrying")
                consumer.unsubscribe()
                consumer.subscribe(
                    topics=[self.topic_id], 
                    on_assign = self.on_consumer_assigned
                )
                return
            least_offset = min(map(lambda x: x.offset, committed_messages))

            if  least_offset != ConsumerAutoScaler.OFFSET_OUT_OF_RANGE:
                least_offset_index = committed_messages.index(least_offset)
                consumer.seek(committed_messages[least_offset_index])

            print(f"Consumer is assigned, consuming")
            self.consumer_is_assigned = True
        except Exception as e:
            print(e)


    def on_revoke(self, consumer, partitions):
        #TODO: handle if needed in the future
        print("Got revoked")
        print(consumer,partitions)

    def on_lost(self, consumer, partitions):
        #TODO: handle if needed in the future
        print("Got lost")
        print(consumer, partitions)

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
                print("Starting producer")
                await producer.start()

            # initial start
            print("Starting consumer")
            await consumer.start()
            while not partition_is_enough:
                try:
                    group_id_total_consumer = self._group_id_total_consumer(consumer_group) 
                    topic_id_total_partition = self._topic_id_total_partition(consumer_topic)

                    print(f"\ngroup_id_total_consumer: {group_id_total_consumer}\ntopic_id_total_partition:{topic_id_total_partition}")

                    partition_is_enough = topic_id_total_partition >= group_id_total_consumer

                    if not partition_is_enough:
                        print(f"Adding {group_id_total_consumer} partition to topic {consumer_topic}")
                        self._add_partition_on_topic(consumer_topic, group_id_total_consumer)

                except Exception as e:
                    print(e)

                    
                await asyncio.sleep(self._sleep_time)
            

            while len(consumer.assignment()) == 0:
                print("No partition assigned for retrying")
                consumer.unsubscribe()
                consumer.subscribe([consumer_topic])
                await asyncio.sleep(self._sleep_time)


            print("Successfully assigned, consuming")
            await consumer.seek_to_end()

class ProducerObserver:

    def __init__(self, producer_topic: str):
        self._producer_topic = producer_topic 


    def send_input_output_ratio_pair(self, source_topic: str, target_topic: str, send: Callable[[str, bytes], Any], topic_configs: TopicDeploymentConfig):
        '''
        Identify which topic pair should the observer ratio with
        send: a produce function from kafka
        '''
        if source_topic is not None:
            return send(
                self._producer_topic,
                self._input_output_pair_data_format(
                    source_topic,
                    target_topic,
                    topic_configs
                )
            )
    
    def send_total_successful_messages(
            self,
            target_topic: str,
            total_messages: int,
            send: Callable[[str, bytes], Any]
        ):
        if target_topic is not None:
            data = self._success_timestamp_data_format(target_topic, total_messages)
            return send(
                self._producer_topic,
                data
            )

    def _input_output_pair_data_format(self, source_topic, target_topic: str, deploy_configs: TopicDeploymentConfig):
        return InputOutputThroughputPair(
            source_topic=source_topic,
            target_topic=target_topic,
            deploy_configs=deploy_configs
        ).model_dump_json().encode()
        
    def _success_timestamp_data_format(self, target_topic: str, total_messages: int):
        return MachineConditionData(
            target_topic=target_topic,
            total_messages=total_messages,
            timestamp=int(get_timestamp().timestamp())
        ).model_dump_json().encode()

class DeployScripts:

    def __init__(self):
        self._deploy_functions: dict[CloudProvider, Callable[[TopicDeploymentConfig], Coroutine[Any, Any, DeployResult]]] = {}
        self._deploy_functions[CloudProvider.LOCAL] = self._local_deployment
        self._deploy_functions[CloudProvider.GOOGLE_CLOUD] = self._gooogle_deployment

    def get_deploy_functions(self, cloud_provider: CloudProvider):
        return self._deploy_functions[cloud_provider]

    def set_deploy_functions(self, cloud_provider: CloudProvider, deploy_function: Callable[[TopicDeploymentConfig], Coroutine[Any, Any, DeployResult]]):
        self._deploy_functions[cloud_provider] = deploy_function

    async def _local_deployment(self, configs: TopicDeploymentConfig) -> DeployResult:
        pass

    async def _gooogle_deployment(self, configs: TopicDeploymentConfig) -> DeployResult:
        pass

class AutoDeployer(MasterObserver):

    def __init__(
            self,
            deploy_script: DeployScripts,
            should_be_deployed : Callable[[str, int] ,bool],
            deploy_delay: int
        ):
        """
        A class to facilitate the auto deployment process for each consumer topic.

        Args:
            deploy_machine (Callable): A function used to deploy based on topic configurations. The topic configuration
                must include the topic consumer. It should return two things: machine_id and a callback function for
                shutting down the machine.
            should_be_deployed (Callable): A function that receives topic_id and topic_throughput. Use this function to
                decide when a topic machine should be deployed.
            deploy_delay (int): Delay (in seconds) for topic deployment. Even if a topic's throughput is still low, it won't
                be deployed after several attempts based on this delay.
        """

        self._deploy_scripts = deploy_script
        self._should_be_deployed = should_be_deployed
        self._deploy_delay = deploy_delay

        self._topic_deployment_configs: dict[str, TopicDeploymentConfig] = {}
        self._machine_ids: list[DeployResult] = []
        self._can_deploy_topic: dict[str, TopicDeployDelay] = {}

    async def delay_deploy(self, topic_id: str):
        await asyncio.sleep(self._deploy_delay)
        self._can_deploy_topic[topic_id].can_be_deployed = True


    def on_receive(self, data: InputOutputThroughputPair | MachineConditionData):
        if isinstance(data, MachineConditionData):
            return
        
        if data.deploy_configs:
            self._topic_deployment_configs[data.deploy_configs.topic_id] = data.deploy_configs
            self._can_deploy_topic[data.deploy_configs.topic_id] = TopicDeployDelay(
                can_be_deployed=True,
                deployed_timestamp=get_timestamp()
            )

    async def start(self):
        pass
    
    async def deploy(self, source_topic: str, source_total_calls: int, target_topic: str) -> bool:
        try:
            
            if target_topic not in self._can_deploy_topic:
                print(f"Topic {target_topic} does not exist, might be not sending heartbeat during initial start or does not have deployment configs")
                return False

            async with self._can_deploy_topic[target_topic].lock:
                if not self._can_deploy_topic[target_topic].can_be_deployed:
                    time_remaining = get_timestamp() - self._can_deploy_topic[target_topic].deployed_timestamp
                    print(f"Cannot be deployed yet, time remaining: {time_remaining}")
                    return False

                if self._should_be_deployed(source_topic, source_total_calls):
                    machine_deployer = self._deploy_scripts.get_deploy_functions(
                        self._topic_deployment_configs[target_topic].cloud_deploy_configs.provider
                    )
                    deploy_result = await machine_deployer(self._topic_deployment_configs[target_topic])
                    self._machine_ids.append(deploy_result)
                    self._can_deploy_topic[target_topic].can_be_deployed = False
                    self._can_deploy_topic[target_topic].deployed_timestamp = get_timestamp()
                    asyncio.create_task(self.delay_deploy(target_topic))
                    return True
            
                print("Machine should not be deployed, could be a spike")
                return False

        except Exception as e:
            print(e)
            return False
    
    async def stop(self):
        for deploy_result in self._machine_ids:
            await deploy_result.shut_down_machine(deploy_result.machine_id)

class TopicSpikeChecker:

    def __init__(self, topic_statistic: TopicStatistic):
        self._topic_statistic = topic_statistic

    def check_spike_by_z_value(self, z_threshold: int, topic_id: str, topic_throughput: int) -> bool:
        print(f"Checking if topic {topic_id} is a spike or not")
        std = self._topic_statistic.get_topic_standard_deviation(topic_id)
        mean = self._topic_statistic.get_topic_mean(topic_id)
        z_score = (topic_throughput - mean)/std

        print(f"Topic {topic_id} statistics:\nMean: {mean}\nStandard Deviation: {std}\nZ-Score:{z_score}")
        #TODO: put explanation probably whether it's most likely can be deployed or not
        return z_score > z_threshold

