
import asyncio

from asyncio.subprocess import PIPE, STDOUT 

async def main():
    cmd = 'python -m analyzer'
    print("running script ")
    process = await asyncio.create_subprocess_shell(cmd, stdin = PIPE, stdout = PIPE, stderr = STDOUT)
    print("Done running process")
    # Open a file to write stdout to
    if process.stdout:
        while True:
            curr_line = await process.stdout.readline()
            print(str(curr_line))

    try:
        await process.wait()
    except KeyboardInterrupt:
        process.kill()

if __name__ == "__main__":
    asyncio.run(main())
