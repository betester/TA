from abc import ABC, abstractmethod
from typing import Optional
from pydantic import BaseModel

class CrawlerResponse(BaseModel):
    message: str
    source: str
    timestamp: float

class Crawler(ABC):

    @abstractmethod
    async def crawl(self) -> Optional[CrawlerResponse]:
        pass
