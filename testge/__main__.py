

from asyncio import get_event_loop
from testge.dynamic_partition import run_test


async def main():
    await run_test()

if __name__ == "__main__":
    loop = get_event_loop()
    loop.run_until_complete(main())
