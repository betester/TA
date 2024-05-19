

from traceback import print_exc

from asyncio import create_task, sleep, gather
from confluent_kafka.admin import AdminClient, NewTopic
from transformers.utils.hub import uuid4
from analyzer.analyzer import DisasterAnalyzerImpl
from analyzer.processor import AnalyzerProcessor
from fogverse import Producer
from fogverse.base import Processor
from fogverse.consumer_producer import ConfluentConsumer, ConfluentProducer
from fogverse.general import ParallelRunnable
from master.component import MasterComponent
from master.contract import TopicDeploymentConfig
from master.master import DeployScripts
from fogverse.fogverse_logging import get_logger
from analyzer.component import AnalyzerComponent
from model.crawler_contract import CrawlerResponse

deploy_scripts = DeployScripts() 

# skenario 1 : 10 konsumer 30 detik   
# skenario 2 : 10 konsumer 1 detik   
# skenario 3 : 3 konsumer 1 detik   
# skenario 4 : 3 konsumer 30 detik   

total_consumers = 10
time_interval = 5
send_rate = 0.01 # 50 messages per second
consume_rate = 0.5
initial_partition = 2
current_consumers = 0 

from confluent_kafka.admin import (
    AdminClient
)

topic = "h"
group_id = "he"
kafka_server = "localhost:9092"
consumer_topic = "cleint"

master_component = MasterComponent()
analyzer_component = AnalyzerComponent()

class MockSender(Producer):

    def __init__(self, producer_observer):
        self.producer_servers = kafka_server
        self.producer_topic = topic
        self.producer_conf = {
                "metadata_max_age_ms": 5 * 1000 # 5 seconds to refresh the metadata
        }
        self.producer_observer = producer_observer
        self._closed = False

        Producer.__init__(self)

    async def receive(self):
        await sleep(send_rate)
        return CrawlerResponse(
            message="help fire happens in toronto",
            source="BBC"
        )  

    def encode(self, data):
        return CrawlerResponse.model_dump_json(data).encode()

    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        result = await super().send(data, topic, key, headers, callback)
        self.producer_observer.send_total_successful_messages(
            target_topic=self.producer_topic,
            total_messages=1
        )
        self.producer_observer.send_expected_consumer(
            expected_consumer=current_consumers
        )

        return result


class MockProcessor(Processor):

    def process(self, messages: list[bytes]) -> list[bytes]:
        processed_msg : list[bytes] = [] 

        x = 0

        for i in range(1_000_000):
            x += ((x + i) % 1129)  * 1227

        for message in messages:
            msg_val = message
            processed_msg.append(msg_val)

        return processed_msg



def run_mock_consumer(extra_args):

        mock_processor = MockProcessor()

        if extra_args == "GPU":
            disaster_analyzer = DisasterAnalyzerImpl(
                ("is_disaster", "./mocking_bird")
            )
            mock_processor = AnalyzerProcessor(disaster_analyzer)

        total_producer = int(input("max process size: "))

        producer_observer = master_component.producer_observer()

        mock_consumer = ConfluentConsumer(
            topic,
            kafka_server,
            group_id,
            str(uuid4()),
            None,
            producer_observer,
            consumer_extra_config={
                "auto.offset.reset": "latest",
                "metadata.max.age.ms": 5 * 1000 # 5 seconds to refresh the metadata
            }
        )

        mock_producer_con = ConfluentProducer(
            consumer_topic, 
            kafka_server,
            mock_processor,
            producer_observer,
            max_process_size=total_producer
        )

        parallel_runnable = ParallelRunnable(
            mock_consumer,
            mock_producer_con,
            None
        )

        parallel_runnable.run()

async def run_multithreading_test():

    running_node = []

    master = master_component.multithreading_master()
    producer_observer = master_component.producer_observer()

    mock_producer = MockSender(producer_observer)

    running_node.append(create_task(mock_producer.run()))
    running_node.append(create_task(master.run()))

    await gather(*running_node)
    
async def run_dynamic_partition_test():

    global current_consumers
    running_consumers = []
    deleted_topic = [topic, 'observer']

    kafka_admin = AdminClient(
        conf={
            "bootstrap.servers": "localhost"
        },
    )

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

    producer_observer = master_component.producer_observer()
    mock_producer = MockSender(producer_observer)

    try:

        master = master_component.dynamic_partition_master_observer(consumer_topic, topic, kafka_admin, 1, "not related")

        running_master = create_task(master.run())
        producer = create_task(mock_producer.run())

        while current_consumers < total_consumers:
            deploy = deploy_scripts.get_deploy_functions("LOCAL")
            logger = get_logger(name=f"testge-{current_consumers}")

            result = await deploy(
                TopicDeploymentConfig(
                    max_instance=1,
                    topic_id=topic,
                    project_name="test",
                    image_name="test",
                    zone="test",
                    service_account="test",
                    image_env={"env": "ye"},
                    machine_type="type",
                    provider="provider",
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

