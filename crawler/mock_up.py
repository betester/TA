
import asyncio

from uuid import UUID, uuid4

from crawler import Crawler
from random import randint


class MockUpCrawler(Crawler):
    
    '''
    A mock up crawler class which will randomly select between (fastest_time and slowest_time)
    fastest_time (int) the fastest time the crawler finish in second 
    slowest_time (int) the slowest_time the crawler finish in second
    '''
    def __init__(self, fastest_time, slowest_time):
        self.fastest_time = fastest_time
        self.slowest_time = slowest_time

    async def crawl(self) -> UUID:
        random_wait_time = randint(self.fastest_time, self.slowest_time)
        await asyncio.sleep(random_wait_time)
        return uuid4()
    
    
