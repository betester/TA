
from analyzer import DisasterAnalyzer, DisasterAnalyzerResponse
from typing import Optional
from transformers import BertTokenizer, BertForSequenceClassification

from fogverse.fogverse_logging import get_logger

import joblib
import torch


class DisasterAnalyzerImpl(DisasterAnalyzer):

    def __init__(self, model_source: str):
        self._model: BertForSequenceClassification = joblib.load(model_source)
        self._tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
        self.__log = get_logger(name=self.__class__.__name__)
        self._model.eval()


    async def analyze(self, text: str) -> Optional[DisasterAnalyzerResponse]:
        try:
            tokenized_text = self._tokenizer(text, padding=True, truncation=True, return_tensors='pt')
            with torch.no_grad():
                outputs = self._model(**tokenized_text)
                # Get the predicted class
                predicted_class = torch.argmax(outputs.logits, dim=1)
                
            return DisasterAnalyzerResponse(
                location="YO",
                category="YO",
                is_disaster=predicted_class[0].item() == 1
            )
                
        except Exception as e:
            self.__log.error(e)


