import asyncio
from aiokafka import AIOKafkaProducer

async def produce():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        client_id='aiokafka-producer'
    )
    
    # Setup and start the producer
    await producer.start()

    try:
        # Produce a message to the 'example-topic'
        await producer.send_and_wait(topic='example-topic', value=b'Hello, Kafka!')
    finally:
        # Gracefully stop the producer
        await producer.stop()

# Run the producer
asyncio.run(produce())
