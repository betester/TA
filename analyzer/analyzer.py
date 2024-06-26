from model.analyzer_contract import DisasterAnalyzer
from typing import Optional, Tuple
from transformers import BertTokenizer, BertForSequenceClassification

from fogverse.fogverse_logging import get_logger

import torch
import torch.nn.functional as F

class DisasterAnalyzerImpl(DisasterAnalyzer):

    def __init__(self, *model_source: Tuple[str, str]):
        self.device = torch.device("cpu")
        self._models: dict[str, BertForSequenceClassification] = self._assign_model(*model_source)
        self._tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
        self.__log = get_logger(name=self.__class__.__name__)

    def analyze(self, attribute: str, text: list[str]) -> list[str]:
        try:
            tokenized_text = self._tokenizer.batch_encode_plus(
                text,
                max_length=64,
                add_special_tokens=True,
                return_token_type_ids=False, 
                padding="max_length",
                truncation = True,
                return_attention_mask=True, 
                return_tensors='pt'
            )
            tokenized_text.to(self.device)
            model = self._models[attribute]
            id2label = model.config.id2label 
            
            with torch.no_grad():
                outputs = model(**tokenized_text)
                # Get the predicted class
                out = F.softmax(outputs.logits, dim=1)
                predicted_classes = torch.argmax(out, dim=1)
                return [id2label[i.item()] for i in predicted_classes] 

        except Exception as e:
            self.__log.error(e)
            return []

    def _assign_model(self, *model_sources: Tuple[str, str]) -> dict[str, BertForSequenceClassification]:
        
        models: dict[str, BertForSequenceClassification] = {}
        for attribute, model_source in model_sources:
            model = BertForSequenceClassification.from_pretrained(model_source)
            if type(model) == BertForSequenceClassification:
                model.to(self.device)
                models[attribute] = model
        return models
