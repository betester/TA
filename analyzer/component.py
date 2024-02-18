from typing import Optional
from fogverse.util import get_config
from master.master import ConsumerAutoScaler
from .analyzer import DisasterAnalyzerImpl
from .producer import AnalyzerProducer

class AnalyzerComponent:

    def __init__(self):
        self._producer_topic = str(get_config("ANALYZER_PRODUCER_TOPIC", self, "client_v6"))
        self._producer_servers = str(get_config("ANALYZER_PRODUCER_SERVERS", self, "localhost:9092"))
        self._consumer_topic = str(get_config("ANALYZER_CONSUMER_TOPIC", self, "analyze_v1"))
        self._consumer_servers = str(get_config("ANALYZER_CONSUMER_SERVERS", self, "localhost:9092"))
        self._consumer_group_id = str(get_config("ANALYZER_CONSUMER_GROUP_ID", self, "analyzer_v2"))
        # assigns based on the attribute and model source
        self._disaster_classifier_model_source = ("is_disaster", str(get_config("DISASTER_CLASSIFIER_MODEL_SOURCE", self, "./mocking_bird")))
        self._keyword_classifier_model_source = ("keyword", str(get_config("KEYWORD_CLASSIFIER_MODEL_SOURCE", self, "./jay_bird")))

    def disaster_analyzer(self, consumer_auto_scaler: Optional[ConsumerAutoScaler]):
        
        disaster_analyzers = DisasterAnalyzerImpl(
            self._disaster_classifier_model_source,
            self._keyword_classifier_model_source
        )

        analyzer_producer = AnalyzerProducer(
            producer_topic=self._producer_topic,
            producer_servers=self._producer_servers, 
            consumer_topic=self._consumer_topic, 
            consumer_servers=self._consumer_servers, 
            classifier_model=disaster_analyzers,
            consumer_group_id=self._consumer_group_id,
            consumer_auto_scaler=consumer_auto_scaler
        )

        return analyzer_producer
