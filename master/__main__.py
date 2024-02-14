
from aiokafka.client import asyncio

from master.component import MasterComponent

async def main():
    loop = asyncio.get_event_loop()

    master_component = MasterComponent()

    consumer_partition_auto_scaler = master_component.consumer_partition_auto_scaler()
    partition_auto_scaler_task = loop.create_task(consumer_partition_auto_scaler.run())

    return asyncio.gather(
        partition_auto_scaler_task
    )

if __name__ == "__main__":
    asyncio.run(main()) 
