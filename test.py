
from aiokafka.consumer.fetcher import asyncio
from confluent_kafka import Message
from fogverse.base import Processor
from fogverse.consumer_producer import ConfluentConsumer, ConfluentProducer
from fogverse.general import ParallelRunnable

class TestProcessor(Processor):
    def process(self, message: Message):
        return b'Hello World'

def main():
    consumer = ConfluentConsumer(
        topics=["client"],
        kafka_server="localhost:9092",
        consumer_extra_config={
            'auto.offset.reset': 'latest',
            'group.id': 'testing_client'
        },
        poll_time=0.01
    )
    
    producer = ConfluentProducer(
        topic="client",
        kafka_server="localhost:9092",
        processor=TestProcessor()

    runnable = ParallelRunnable(
        consumer,
        producer,
        None,
        total_producer=5
    )

    runnable.run()



if __name__ == "__main__":
    main()
