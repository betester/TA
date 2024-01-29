from abc import ABC, abstractmethod
from uuid import UUID


class Crawler(ABC):

    @abstractmethod
    async def crawl(self) -> UUID:
        pass
