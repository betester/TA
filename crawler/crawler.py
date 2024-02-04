import csv

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
            return self._parser(next(self._files))
        except:
            self._set_up_csv_reader()
            
    def _set_up_csv_reader(self):
        curr_file = next(self._files)
        self._current_file_reader = csv.reader(curr_file)
