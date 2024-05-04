
from traceback import print_exc

from asyncio import Task, create_task, gather, sleep
from confluent_kafka.admin import AdminClient
from fogverse import Consumer, Producer
from master.component import MasterComponent

total_consumers = int(input("total consumers: "))
time_interval = float(input("time interval in seconds: "))
send_rate = float(input("send rate from producer in seconds: "))
initial_partition = 1
current_consumers = 0 

from confluent_kafka.admin import (
    AdminClient,
    NewTopic
)

topic = "testge"
group_id = "testge"
kafka_server = "localhost:9092"
master_component = MasterComponent()


kafka_admin = AdminClient(
    conf={
        "bootstrap.servers": "localhost"
    },
)

master = master_component.dynamic_partition_master_observer(topic, kafka_admin, 1)
producer_observer = master_component.producer_observer()
consumer_auto_scaler = master_component.consumer_auto_scaler()

class MockSender(Producer):

    def __init__(self):
        self.producer_servers = kafka_server
        self.producer_topic = topic
        self.group_id = group_id

        Producer.__init__(self)

    async def receive(self):
        await sleep(send_rate)
        return "mocking message" 

    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        result = await super().send(data, topic, key, headers, callback)
        await producer_observer.send_total_successful_messages_async(
            target_topic="",
            total_messages=0,
            expected_consumer=current_consumers,
            send=self.producer.send
        )

        return result


class MockConsumer(Consumer, Producer):
    
    def __init__(self):
        self.consumer_topic = topic
        self.consumer_servers = kafka_server 
        self.group_id = group_id 

        Consumer.__init__(self)

    async def _start(self):
        await consumer_auto_scaler.async_start(
            consumer=self.consumer,
            consumer_group=self.group_id,
            consumer_topic=self.consumer_topic
        )

        await producer_observer.send_input_output_ratio_pair(
            source_topic=self.consumer_topic,
            target_topic="",
            topic_configs=None,
            send = lambda x, y: self.producer.send(topic=x, value=y)
        )

mock_consumer = MockConsumer()
mock_producer = MockSender()

async def run_test():

    global current_consumers

    try:
        print(f"Deleting topic {topic}")
        delete_topic_future = kafka_admin.delete_topics([topic])[topic]
        delete_topic_future[topic].result()

        print(f"Creating topic {topic}")
        create_topic_future = kafka_admin.create_topics([NewTopic(topic, num_partitions=initial_partition)])[topic]
        create_topic_future.result()

    except Exception:
        print_exc()

    running_consumers : list[Task] = []
    producer = create_task(mock_producer.run())

    while current_consumers < total_consumers:
        await sleep(time_interval)
        running_consumers.append(create_task(mock_consumer.run()))
        current_consumers += 1

    await gather(producer, *running_consumers)
