
import argparse
from asyncio import get_event_loop
from testge.testge import run_mock_consumer, run_dynamic_partition_test, run_multithreading_test


async def main():
    parser = argparse.ArgumentParser(description="Set up test that needed to be run")
    parser.add_argument('--type', metavar='path', required=True)
    parser.add_argument('--extra', metavar='path')

    args = parser.parse_args()

    if args.type == 'dp':
        if args.extra == 'consume':
            run_mock_consumer(None)
        else:
            await run_dynamic_partition_test()
    if args.type == 'mt':
        if args.extra != None:
            run_mock_consumer(args.extra)
        else:
            await run_multithreading_test()

if __name__ == "__main__":
    loop = get_event_loop()
    loop.run_until_complete(main())
