from abc import ABC, abstractmethod
from typing import Optional
from pydantic import BaseModel

# re-exporting the file
from .crawler import MockUpCrawler


class CrawlerResponse(BaseModel):
    message: str
    source: str

class Crawler(ABC):

    @abstractmethod
    async def crawl(self) -> Optional[CrawlerResponse]:
        pass
