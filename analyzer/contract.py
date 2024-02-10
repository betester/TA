
from abc import ABC, abstractmethod
from typing import Optional
from pydantic import BaseModel
from transformers.feature_extraction_utils import torch


class DisasterAnalyzerResponse(BaseModel):
    keyword: Optional[str] = None
    is_disaster: bool

    

class DisasterAnalyzer(ABC):

    @abstractmethod
    async def analyze(self, attribute: str, text: str) -> Optional[torch.Tensor]:
        pass
