from fogverse.util import get_config
from .analyzer import DisasterAnalyzerImpl
from .producer import AnalyzerProducer

class AnalyzerComponent:

    def __init__(self):
        self._producer_topic = str(get_config("ANALYZER_PRODUCER_TOPIC", self, "client"))
        self._producer_servers = str(get_config("ANALYZER_PRODUCER_SERVERS", self, "localhost:9092"))
        self._consumer_topic = str(get_config("ANALYZER_CONSUMER_TOPIC", self, "analyze"))
        self._consumer_servers = str(get_config("ANALYZER_CONSUMER_SERVERS", self, "localhost:9092"))
        self._consumer_group_id = str(get_config("ANALYZER_CONSUMER_GROUP_ID", self, "analyzer"))
        self._model_source = str(get_config("MODEL_SOURCE", self, "./mocking_bird.joblib"))


    def disaster_analyzer(self):
        analyzer = DisasterAnalyzerImpl(self._model_source)
        analyzer_producer = AnalyzerProducer(
            producer_topic=self._producer_topic,
            producer_servers=self._producer_servers, 
            consumer_topic=self._consumer_topic, 
            consumer_servers=self._consumer_servers, 
            analyzer=analyzer,
            consumer_group_id=self._consumer_group_id
        )

        return analyzer_producer
