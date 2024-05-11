
import time

from traceback import print_exc

from functools import partial
from confluent_kafka import Message
from asyncio import create_task, gather, sleep
from confluent_kafka.admin import AdminClient, NewTopic
from transformers.utils.hub import uuid4
from fogverse import Producer
from fogverse.base import Processor
from fogverse.consumer_producer import ConfluentConsumer, ConfluentProducer
from fogverse.general import ParallelRunnable
from master.component import MasterComponent
from master.contract import TopicDeploymentConfig
from master.master import DeployScripts
from fogverse.fogverse_logging import get_logger

deploy_scripts = DeployScripts() 

total_consumers = 2
time_interval = 30
send_rate = 0.1 
consume_rate = 0.5
initial_partition = 1
current_consumers = 0 

from confluent_kafka.admin import (
    AdminClient
)

topic = "h"
group_id = "he"
kafka_server = "localhost:9092"
consumer_topic = "cleint"

master_component = MasterComponent()

kafka_admin = AdminClient(
    conf={
        "bootstrap.servers": "localhost"
    },
)

master = master_component.dynamic_partition_master_observer(consumer_topic, topic, kafka_admin, 1, "not related")
producer_observer = master_component.producer_observer()

class MockSender(Producer):

    def __init__(self):
        self.producer_servers = kafka_server
        self.producer_topic = topic
        self.producer_conf = {
                "metadata_max_age_ms": 5 * 1000 # 5 seconds to refresh the metadata
        }
        self._closed = False

        Producer.__init__(self)

    async def receive(self):
        await sleep(send_rate)
        return "mocking message" 

    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        result = await super().send(data, topic, key, headers, callback)
        await producer_observer.send_total_successful_messages_async(
            target_topic=self.producer_topic,
            total_messages=1,
            expected_consumer=current_consumers,
            send=self.producer.send
        )

        return result

mock_producer = MockSender()

class MockProcessor(Processor):

    def process(self, messages: list[Message]) -> list[bytes]:
        processed_msg : list[bytes] = [] 

        time.sleep(consume_rate)

        for message in messages:
            msg_val = message.value()
            processed_msg.append(msg_val)

        return processed_msg

mock_processor = MockProcessor()

start_producer_callback =partial(
    producer_observer.send_input_output_ratio_pair,
    topic,
    "",
    None
)

def run_mock_consumer():

        kafka_admin = AdminClient(
            conf={
                "bootstrap.servers": "localhost",
            },
        )

        consumer_auto_scaler = master_component.consumer_auto_scaler(kafka_admin)

        mock_consumer = ConfluentConsumer(
            topic,
            kafka_server,
            group_id,
            str(uuid4()),
            consumer_auto_scaler,
            consumer_extra_config={
                "auto.offset.reset": "latest",
                "metadata.max.age.ms": 5 * 1000 # 5 seconds to refresh the metadata
            }
        )

        mock_producer_con = ConfluentProducer(
            consumer_topic, 
            kafka_server,
            mock_processor,
            start_producer_callback,
            producer_observer.send_total_successful_messages
        )

        parallel_runnable = ParallelRunnable(
            mock_consumer,
            mock_producer_con,
            None
        )

        parallel_runnable.run()

async def run_test():

    global current_consumers
    running_consumers = []
    deleted_topic = [topic, 'observer']


    try:
        print(f"Deleting topic {deleted_topic}")
        delete_topic_dictionary = kafka_admin.delete_topics(deleted_topic)
        delete_topic_dictionary[topic].result()
        create_topic_future = kafka_admin.create_topics([NewTopic(topic, num_partitions=initial_partition)])[topic]
        delete_topic_dictionary['observer'].result()
        create_topic_future.result()

    except Exception:
        print_exc()

    await sleep(2)

    try:
        running_master = create_task(master.run())
        producer = create_task(mock_producer.run())

        while current_consumers < total_consumers:
            deploy = deploy_scripts.get_deploy_functions("LOCAL")
            logger = get_logger(name=f"testge-{current_consumers}")

            result = await deploy(
                TopicDeploymentConfig(
                    service_name="testge --type=dp --extra=consume"
                ),
                logger
            )

            running_consumers.append(result.shut_down_machine)
            print(f"iteration {current_consumers}")
            current_consumers += 1
            await sleep(time_interval)

        await gather(running_master, producer)

    except KeyboardInterrupt as e:
        for shutdown in running_consumers:
            shutdown()

