
import argparse
from asyncio import get_event_loop
from testge.dynamic_partition import run_mock_consumer, run_test


async def main():
    parser = argparse.ArgumentParser(description="Set up test that needed to be run")
    parser.add_argument('--type', metavar='path', required=True)
    parser.add_argument('--extra', metavar='path')

    args = parser.parse_args()

    if args.type == 'dp':
        if args.extra == 'consume':
            run_mock_consumer()
        else:
            await run_test()

if __name__ == "__main__":
    loop = get_event_loop()
    loop.run_until_complete(main())
