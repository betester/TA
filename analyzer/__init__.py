
from abc import ABC, abstractmethod
from typing import Optional
from pydantic import BaseModel


class DisasterAnalyzerResponse(BaseModel):
    location: str
    category: str
    is_disaster: bool

    

class DisasterAnalyzer(ABC):

    @abstractmethod
    async def analyze(self, text: str) -> Optional[DisasterAnalyzerResponse]:
        pass
