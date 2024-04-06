import asyncio

from master.component import MasterComponent

async def main():
    master_component = MasterComponent()
    event_handler = master_component.master_event_handler()
    await event_handler.run()

if __name__ == "__main__":
    asyncio.run(main())
