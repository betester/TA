import csv
import asyncio

from typing import Any, Callable, Optional
from crawler import Crawler, CrawlerResponse


class MockUpCrawler(Crawler):
    
    def __init__(self, parser: Callable[[Any], CrawlerResponse], *files):
        self._files = iter(files)
        self._parser = parser
        self._set_up_csv_reader()

    async def crawl(self) -> Optional[CrawlerResponse]:
        '''
        This will throw error when there are no more files 
        left to read, be sure to properly handle the exception
        '''
        try:
            data = self._parser(next(self._current_file_reader))
            # mandatory for  making other to fetch the data
            await asyncio.sleep(0.01)
            return data       
        except:
            self._set_up_csv_reader()
            return await self.crawl()
            
    def _set_up_csv_reader(self):
        curr_file = next(self._files)
        print(curr_file)
        self._current_file_reader = csv.reader(curr_file)
