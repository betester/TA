import asyncio
from aiokafka import AIOKafkaConsumer

async def consume():
    consumer = AIOKafkaConsumer(
        'example-topic',
        bootstrap_servers='localhost:9092',
        group_id='aiokafka-group'
    )

    # Setup and start the consumer
    await consumer.start()

    try:
        # Consume messages from the 'example-topic'
        async for message in consumer:
            print(f"Received message: {message.value.decode('utf-8')}")
        print("test")
    finally:
        # Gracefully stop the consumer
        await consumer.stop()

# Run the consumer
asyncio.run(consume())
