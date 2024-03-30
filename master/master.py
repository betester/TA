from asyncio import StreamReader

import os
import time
from uuid import uuid4

from logging import Logger
from collections.abc import Callable, Coroutine
from typing import Any, Optional
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
from asyncio.subprocess import PIPE, STDOUT 
from confluent_kafka import Consumer, KafkaException, Message, TopicCollection
from fogverse.fogverse_logging import get_logger
from fogverse.util import get_timestamp
from master.contract import (
    DeployArgs,
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
        self.consumer_is_assigned_partition = False

        self._logger = get_logger(name=self.__class__.__name__)
    

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
                error = e.args[0]
                if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART or not error.retriable():
                    return False
                self._logger.info(e)
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

                self._logger.info(e)
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
              /,
              retry_attempt: int=3) -> Optional[list[Message]]:

        self._logger.info("Creating topic if not exist")
        self.topic_id = topic_id

        if not self._topic_exist(topic_id, retry_attempt):
            topic_created = self._create_topic(topic_id, retry_attempt)
            if not topic_created:
                raise Exception("Topic cannot be created")

        self._logger.info(f"Subscribing to topic {topic_id}")

        consumer.subscribe(
            topics=[topic_id] 
        )

        self._logger.info("Waiting for consumer to be assigned on a consumer group")

        while True:
            consumer_member_id = consumer.memberid() 
            if consumer.memberid():
                self._logger.info(f"Consumer assigned to consumer group with id {consumer_member_id}")
                break

            self._logger.info("Consumer is still not assigned on the consumer group, retrying...")
            time.sleep(self._sleep_time)

        consumed_message = []

        while not self.consumer_is_assigned_partition:
            try:
                group_id_total_consumer = self._group_id_total_consumer(group_id) 
                topic_id_total_partition = self._topic_id_total_partition(topic_id)

                self._logger.info(f"\ngroup_id_total_consumer: {group_id_total_consumer}\ntopic_id_total_partition:{topic_id_total_partition}")

                partition_is_enough = topic_id_total_partition >= group_id_total_consumer

                if not partition_is_enough:
                    self._logger.info(f"Adding {group_id_total_consumer} partition to topic {topic_id}")
                    self._add_partition_on_topic(topic_id, group_id_total_consumer)

                message = consumer.poll(self._sleep_time)

                if message:
                    consumed_message.append(message)

                consumer_partition_assignment = consumer.assignment()
                self.consumer_is_assigned_partition = len(consumer_partition_assignment) != 0

                if self.consumer_is_assigned_partition:
                    self._logger.info(f"Consumer is assigned to {len(consumer_partition_assignment)} partitions, consuming")
                    return consumed_message

                self._logger.info("Fail connecting, retrying...")
            except Exception as e:
                self._logger.info(e)


    def on_revoke(self, consumer, partitions):
        #TODO: handle if needed in the future
        self._logger.info("Got revoked")
        self._logger.info(consumer,partitions)

    def on_lost(self, consumer, partitions):
        #TODO: handle if needed in the future
        self._logger.info("Got lost")
        self._logger.info(consumer, partitions)

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
                    self._logger.info(e)

                    
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
        self._logger = get_logger(name=self.__class__.__name__)


    def send_input_output_ratio_pair(self, source_topic: str, target_topic: str, topic_configs: TopicDeploymentConfig, send: Callable[[str, bytes], Any]):
        '''
        Identify which topic pair should the observer ratio with
        send: a produce function from kafka
        '''
        self._logger.info(f"Sending input output ratio to topic {self._producer_topic}")
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

    def __init__(self, log_dir_path: str='logs'):
        self._deploy_functions: dict[str, Callable[[TopicDeploymentConfig, Logger], Coroutine[Any, Any, DeployResult]]] = {}
        self._deploy_functions["LOCAL"] = self._local_deployment
        self._log_dir_path = log_dir_path

        self._logger = get_logger(name=self.__class__.__name__)

    def get_deploy_functions(self, cloud_provider: str):
        return self._deploy_functions[cloud_provider]

    def set_deploy_functions(self, cloud_provider: str, deploy_function: Callable[[TopicDeploymentConfig, Logger], Coroutine[Any, Any, DeployResult]]):
        self._deploy_functions[cloud_provider] = deploy_function
    
    async def write_deployed_service_logs(self, file_name: str, stdout: StreamReader):
        try:
            os.makedirs(self._log_dir_path, exist_ok=True)
            full_file_path = os.path.join(self._log_dir_path, file_name)
            with open(full_file_path, 'w', encoding='utf-8') as f:
                async for line in stdout:
                    f.write(line.decode('utf-8'))
        except Exception as e:
            self._logger.error(f"An error occurred while writing logs: {e}")
    
    async def _write_deploy_logs(self, stdout: StreamReader):
        try:
            async for line in stdout:
                self._logger.info(line.decode('utf-8'))
        except Exception as e:
            self._logger.error(f"An error occurred while writing logs: {e}")
    
    async def _local_deployment(self, configs: TopicDeploymentConfig) -> DeployResult:

        cmd = f'python -m {configs.service_name}'
        self._logger.info(f"Running script: {cmd}")
        process = await asyncio.create_subprocess_shell(
            cmd,
            stdin = PIPE,
            stdout = PIPE, 
            stderr = STDOUT
        )

        async def kill_process_and_task() -> bool:
            self._logger.info(f"Shutting down {process.pid}")
            try:
                process.kill()
                return True
            except Exception as e:
                self._logger.error(f"Fail shutting down process {process.pid}, please turn off them manually", e)
                return False

            
        if process.stdout:
            log_file_name = f'{configs.service_name}-{process.pid}.log'
            self._logger.info(f"Writing service {configs.service_name} logs with filename: {log_file_name}")
            asyncio.create_task(self.write_deployed_service_logs(log_file_name, process.stdout)) 
            self._logger.info(f"Command executed successfully, service {configs.service_name} is deployed")
            return DeployResult(
                machine_id=str(process.pid),
                shut_down_machine= kill_process_and_task
            ) 

        raise Exception("Process cannot be created")

class AutoDeployer(MasterObserver):

    def __init__(
            self,
            deploy_script: DeployScripts,
            should_be_deployed : Callable[[str, float] ,bool],
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

        self._logger = get_logger(name=self.__class__.__name__)
        
        self._topic_deployment_configs: dict[str, TopicDeploymentConfig] = {}
        self._machine_ids: list[DeployResult] = []
        self._can_deploy_topic: dict[str, TopicDeployDelay] = {}
        self._topic_total_deployment: dict[str, int] = {}

    async def delay_deploy(self, topic_id: str):
        await asyncio.sleep(self._deploy_delay)
        self._can_deploy_topic[topic_id].can_be_deployed = True

    def get_topic_total_machine(self, topic: str) -> int:
        return self._topic_total_deployment[topic]

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
    
    async def deploy(self, deploy_args: DeployArgs) -> bool:
        try:

            target_topic, source_topic = deploy_args.target_topic, deploy_args.source_topic  
            target_total_calls, source_total_calls = deploy_args.target_topic_throughput, deploy_args.source_topic_throughput

            if target_topic not in self._can_deploy_topic:
                self._logger.info(f"Topic {target_topic} does not exist, might be not sending heartbeat during initial start or does not have deployment configs")
                return False

            async with self._can_deploy_topic[target_topic]._lock:

                if not self._can_deploy_topic[target_topic].can_be_deployed:
                    time_remaining = get_timestamp() - self._can_deploy_topic[target_topic].deployed_timestamp
                    self._logger.info(f"Cannot be deployed yet, time remaining: {time_remaining}")
                    return False
            
                maximum_topic_deployment = self._topic_deployment_configs[target_topic].max_instance
                current_deployed_replica = self._topic_total_deployment.get(target_topic, 0)

                service_name = self._topic_deployment_configs[target_topic].service_name
                provider = self._topic_deployment_configs[target_topic].provider

                if current_deployed_replica >= maximum_topic_deployment:
                    self._logger.info(
                        f"Cannot deploy service {service_name} exceeds maximum limit.\n" 
                        f"current deployed : {current_deployed_replica}\n"
                        f"maximum replica : {maximum_topic_deployment}"
                    )
                    return False

                source_topic_is_not_spike = self._should_be_deployed(source_topic, source_total_calls)
                target_topic_is_not_spike = self._should_be_deployed(target_topic, target_total_calls) 

                if source_topic_is_not_spike and target_topic_is_not_spike:
                    self._logger.info(f"Deploying new machine for service {service_name} to cloud provider: {provider}")

                    machine_deployer = self._deploy_scripts.get_deploy_functions(
                        self._topic_deployment_configs[target_topic].provider
                    )

                    if machine_deployer is None:
                        self._logger.info(f"No deploy script for {provider}, deployment cancelled (you might need to set up deloy script on component)")
                        return False

                    self._logger.info("Starting deployment script")
                    starting_time = get_timestamp()
                    deploy_result = await machine_deployer(self._topic_deployment_configs[target_topic], self._logger)
                    self._logger.info(f"Deployment finished, time taken: {get_timestamp() - starting_time}")
                        
                    if not deploy_result:
                        self._logger.error(f"Deployment failed for service {service_name}")
                        return False

                    self._machine_ids.append(deploy_result)
                    self._can_deploy_topic[target_topic].can_be_deployed = False
                    self._can_deploy_topic[target_topic].deployed_timestamp = get_timestamp()
                    self._topic_total_deployment[target_topic] = current_deployed_replica + 1
                    asyncio.create_task(self.delay_deploy(target_topic))
                    return True
            
                self._logger.info("Machine should not be deployed, could be a spike")
                return False

        except Exception as e:
            self._logger.error(e)
            return False
    
    async def stop(self):
        for deploy_result in self._machine_ids:
            await deploy_result.shut_down_machine()

class TopicSpikeChecker:

    def __init__(self, topic_statistic: TopicStatistic):
        self._topic_statistic = topic_statistic
        self._logger = get_logger(name=self.__class__.__name__)

    def check_spike_by_z_value(self, z_threshold: int, topic_id: str, topic_throughput: float) -> bool:
        self._logger.info(f"Checking if topic {topic_id} is a spike or not")
        std = self._topic_statistic.get_topic_standard_deviation(topic_id)
        mean = self._topic_statistic.get_topic_mean(topic_id)
        z_score = (topic_throughput - mean)/std

        self._logger.info(f"Topic {topic_id} statistics:\nMean: {mean}\nStandard Deviation: {std}\nZ-Score:{z_score}")
            
        self._logger.info(f"{z_score < z_threshold}")
        return z_score < z_threshold

