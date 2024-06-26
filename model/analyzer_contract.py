from abc import ABC, abstractmethod
from typing import Optional
from pydantic import BaseModel

class DisasterAnalyzerResponse(BaseModel):
    keyword: Optional[str] = None
    is_disaster: Optional[str] = None
    text: Optional[str] = None
    crawler_timestamp: float
    analyzer_timestamp: float

class DisasterAnalyzer(ABC):
    
    @abstractmethod
    def analyze(self, attribute: str, text: list[str]) -> list[str]:
        pass
