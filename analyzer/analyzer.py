
from .contract import DisasterAnalyzer
from typing import Optional, Tuple
from transformers import BertTokenizer, BertForSequenceClassification

from fogverse.fogverse_logging import get_logger

import joblib
import torch


class DisasterAnalyzerImpl(DisasterAnalyzer):

    def __init__(self, *model_source: Tuple[str, str]):
        self._models: dict[str, BertForSequenceClassification] = self._assign_model(*model_source)
        self._tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
        self.__log = get_logger(name=self.__class__.__name__)


    async def analyze(self, attribute: str, text: str) -> Optional[torch.Tensor]:
        try:
            tokenized_text = self._tokenizer(text, padding=True, truncation=True, return_tensors='pt')
            model = self._models[attribute]
            with torch.no_grad():
                outputs = model(**tokenized_text)
                # Get the predicted class
                return torch.argmax(outputs.logits, dim=1)

        except Exception as e:
            self.__log.error(e)

    def _assign_model(self, *model_sources: Tuple[str, str]) -> dict[str, BertForSequenceClassification]:
        
        models: dict[str, BertForSequenceClassification] = {}

        for attribute, model_source in model_sources:
            models[attribute] = joblib.load(model_source)
            models[attribute].eval()

        return models
