from typing import Optional
from analyzer.processor import AnalyzerProcessor
from fogverse.consumer_producer import ConfluentConsumer, ConfluentProducer
from fogverse.general import ParallelRunnable
from fogverse.util import get_config
from master.master import ConsumerAutoScaler, ProducerObserver
from .analyzer import DisasterAnalyzerImpl
from .handler import AnalyzerProducer, ParallelAnalyzerJobService

class AnalyzerComponent:

    def __init__(self):
        self._producer_topic = str(get_config("ANALYZER_PRODUCER_TOPIC", self, "client_v6"))
        self._producer_servers = str(get_config("ANALYZER_PRODUCER_SERVERS", self, "localhost:9092"))
        self._consumer_topic = str(get_config("ANALYZER_CONSUMER_TOPIC", self, "hello"))
        self._consumer_servers = str(get_config("ANALYZER_CONSUMER_SERVERS", self, "localhost:9092"))
        self._consumer_group_id = str(get_config("ANALYZER_CONSUMER_GROUP_ID", self, "analyzer_v2"))
        # assigns based on the attribute and model source
        self._disaster_classifier_model_source = ("is_disaster", str(get_config("DISASTER_CLASSIFIER_MODEL_SOURCE", self, "./mocking_bird")))
        self._keyword_classifier_model_source = ("keyword", str(get_config("KEYWORD_CLASSIFIER_MODEL_SOURCE", self, "./jay_bird")))

    def disaster_analyzer(self, consumer_auto_scaler: Optional[ConsumerAutoScaler], producer_observer: ProducerObserver):
        
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
            consumer_auto_scaler=consumer_auto_scaler,
            producer_observer=producer_observer
        )

        return analyzer_producer
    
    def parallel_disaster_analyzer(self, consumer_auto_scaler: ConsumerAutoScaler):

        disaster_analyzers = DisasterAnalyzerImpl(
            self._disaster_classifier_model_source
        )

        analyzer_processor = AnalyzerProcessor(disaster_analyzers)

        consumer = ConfluentConsumer(
            topics=self._consumer_topic,
            kafka_server=self._consumer_servers,
            group_id=self._consumer_group_id,
            consumer_auto_scaler=consumer_auto_scaler,
            consumer_extra_config={
                'session.timeout.ms': 6000,
                'auto.offset.reset': 'latest'
            }
        )

        producer = ConfluentProducer(
            topic=self._producer_topic,
            kafka_server=self._producer_servers,
            processor=analyzer_processor,
            batch_size=20
        )

        runnable = ParallelRunnable(
            consumer,
            producer,
            None,
            total_producer=5
        )

        return ParallelAnalyzerJobService(runnable)

